import asyncio
import ctypes
from dataclasses import dataclass
import logging
import threading
import time
from typing import Any, Callable, Optional


THREAD_NOT_ALIVE_ERROR = """thread: {name}
The thread is not in the RUNNING state.
"""

THREADSAFE_CALL_ERROR = """thread: {name}
Error:  Threadsafe call failed after {timeout_s} seconds, because the service has not
        set the self.service_running event.
"""

SELF_CHECK_WARNING = """thread: {name}
Warning: Self check failed. Service did not set the self.service_running event.
Hint:   After initializing itself, the service must call self.service_running.set()
        If initialization is expected to take longer than {init_timeout} seconds,
        it must assign the appropriate timeout before the thread is started. E.g.:

        class MyService(AioServiceThread):
            def __init__(self, name, param1, param2):
                super().__init__(name=name)
                # ...
                self.init_timeout = 22.5  # seconds
                # ...

            async def _arun(self):
                # initialize the service
                # ...
                self.service_running.set()

                # wait until completed
                await self._astop_event.wait()

                # shut down the service
                # ...
"""


@dataclass
class _WrappedResponse:
    done_event: threading.Event
    result: Any = None
    exception: BaseException = None


class AioServiceThread(threading.Thread):

    logger: logging.Logger

    init_timeout: float = 11.25
    service_running: threading.Event

    _aloop: asyncio.AbstractEventLoop = None
    _astop_event: asyncio.Event = None
    _atasks: list[asyncio.Task]

    _force_stop_timeout: float = 11.25
    _force_stop_sentinel: Exception

    class InjectedException(Exception):
        initial_exc: Exception = None

        def __str__(self) -> str:
            return f"<{self.__class__.__name__} initial_exc={repr(self.initial_exc)}>"

    def __init__(self, name: Optional[str] = None) -> None:
        super().__init__(name=name, daemon=False)

        self._atasks = []

        self.logger = logging.getLogger(f"{self.__class__.__name__}.{self.name}")
        self.logger.setLevel(logging.getLogger(__name__).level)

        self.service_running = threading.Event()

    def log(self, msg: dict, *, level: int = logging.INFO) -> None:
        if isinstance(msg, dict):
            self.logger.log(level, {"thread": self.name, **msg})
        else:
            self.logger.log(level, msg)

    def run(self) -> None:
        self._force_stop_sentinel = Exception("force_stop_sentinel")
        try:
            asyncio.run(self._arun_proxy())
        except self.InjectedException as exc:
            if exc.initial_exc == self._force_stop_sentinel:
                self.log({"event_type": "thread_force_stopped"})
            else:
                raise exc

    @classmethod
    def threadsafe_method(cls, func) -> Callable:

        def wrapper(self: cls, *args, **kwargs) -> Any:
            # pylint: disable=protected-access

            self._wait_for_aloop_initialization()

            response = _WrappedResponse(threading.Event())

            def func_w_embedded_args() -> None:
                try:
                    response.result = func(self, *args, **kwargs)
                except BaseException as exc:  # pylint: disable=broad-exception-caught
                    response.exception = exc
                response.done_event.set()

            self._aloop.call_soon_threadsafe(func_w_embedded_args)
            response.done_event.wait()

            if response.exception is not None:
                raise response.exception
            return response.result

        return wrapper

    def _threadsafe(self, func) -> Callable:

        def wrapper(*args, **kwargs):
            self._wait_for_aloop_initialization()

            response = _WrappedResponse(threading.Event())

            def func_w_embedded_args():
                try:
                    response.result = func(*args, **kwargs)
                except BaseException as exc:  # pylint: disable=broad-exception-caught
                    response.exception = exc
                response.done_event.set()

            self._aloop.call_soon_threadsafe(func_w_embedded_args)
            response.done_event.wait()

            if response.exception is not None:
                raise response.exception
            return response.result

        return wrapper

    def _raise_in_thread(self, exc: object):
        # An example for raising an exception in a thread was shown in:
        #   https://gist.github.com/liuw/2407154
        #
        # This method is used to force stop a service that did not exit after a
        # graceful stop event.

        class _ExcWrapper(self.InjectedException):
            def __init__(self, *args: object) -> None:
                super().__init__(*args)
                self.initial_exc = exc

        if not self.is_alive():
            return

        thread_id = self.ident

        ret = ctypes.pythonapi.PyThreadState_SetAsyncExc(
            ctypes.c_long(self.ident), ctypes.py_object(_ExcWrapper)
        )

        if ret == 0:
            raise ValueError(f"Invalid thread thread_id={thread_id}")
        if ret > 1:
            ctypes.pythonapi.PyThreadState_SetAsyncExc(thread_id, 0)
            raise SystemError(
                f"PyThreadState_SetAsyncExc failed ret={ret} thread_id={thread_id}"
            )

        self.log(
            {"event_type": "exc_injected", "exc": str(exc)},
            level=logging.DEBUG,
        )

    def stop_and_join(
        self, timeout: float = None, then_force_stop: bool = True
    ) -> None:

        @self._threadsafe
        def request_stop():
            self._astop_event.set()

        if not self.is_alive():
            return

        request_stop()
        self.join(timeout=timeout)

        if not self.is_alive():
            return

        if then_force_stop:
            self._raise_in_thread(self._force_stop_sentinel)
            self.join(timeout=self._force_stop_timeout)

            if self.is_alive():
                msg = (
                    f"Force stopping thread '{self.name}' failed after "
                    f"{self._force_stop_timeout} seconds"
                )
                raise RuntimeError(msg)

    def _create_task(self, coro, *, name=None) -> asyncio.Task:
        task = asyncio.create_task(coro, name=name)
        self._atasks.append(task)
        return task

    async def _cancel_tasks(self):
        self.log({"event_type": "cancel_tasks_starting"}, level=logging.DEBUG)

        for task in self._atasks:
            if task.done():
                continue
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        self.log({"event_type": "cancel_tasks_done"}, level=logging.DEBUG)

    def cancel_thread_tasks(self) -> None:
        @self._threadsafe
        def cancel_tasks():
            asyncio.create_task(self._cancel_tasks(), name="_cancel_tasks")

        cancel_tasks()

    def _wait_for_aloop_initialization(self):
        if self.is_alive():  # and not self._astop_event.is_set():
            timeout_s = self.init_timeout
            if not self.service_running.wait(timeout_s):
                error_msg = THREADSAFE_CALL_ERROR.replace("{name}", self.name)
                error_msg = error_msg.replace("{timeout_s}", str(timeout_s))
                self.logger.error(error_msg)
                raise RuntimeError(error_msg)
        else:
            error_msg = THREAD_NOT_ALIVE_ERROR.replace("{name}", self.name)
            self.logger.error(error_msg)
            raise RuntimeError(error_msg)

    async def _ensure_service_is_running(self):
        check_started_time = time.monotonic()
        while (
            not self._astop_event.is_set()
            and not self.service_running.is_set()
            and time.monotonic() - check_started_time < self.init_timeout
        ):
            await asyncio.sleep(0.1)

        if self.service_running.is_set():
            self.log({"event_type": "service_running"}, level=logging.DEBUG)
            return

        if self._astop_event.is_set():
            return

        if not self.service_running.is_set():
            error_msg = SELF_CHECK_WARNING.replace("{name}", self.name)
            error_msg = error_msg.replace("{init_timeout}", str(self.init_timeout))
            self.logger.warning(error_msg)

    async def _arun_proxy(self) -> None:
        self.log({"event_type": "arun_starting"}, level=logging.DEBUG)

        self._aloop = asyncio.get_event_loop()
        self._astop_event = asyncio.Event()

        arun_task = asyncio.create_task(self._arun(), name="_arun")

        await self._ensure_service_is_running()

        await self._astop_event.wait()

        try:
            await arun_task
        except asyncio.CancelledError:
            self.logger.exception({"event_type": "arun_got_canceled"}, stack_info=True)

        self.log({"event_type": "arun_done"}, level=logging.DEBUG)

    async def _arun(self):
        pass
