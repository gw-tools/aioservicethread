import asyncio
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

    def __init__(self, name: Optional[str] = None) -> None:
        super().__init__(name=name, daemon=False)

        self.logger = logging.getLogger(f"{self.__class__.__name__}.{self.name}")
        self.logger.setLevel(logging.getLogger(__name__).level)

        self.service_running = threading.Event()

    def log(self, msg: dict, *, level: int = logging.INFO) -> None:
        if isinstance(msg, dict):
            self.logger.log(level, {"thread": self.name, **msg})
        else:
            self.logger.log(level, msg)

    def run(self) -> None:
        asyncio.run(self._arun_proxy())

    @classmethod
    def threadsafe_method(cls, func) -> Callable:
        no_result = object()

        def wrapper(self: cls, *args, **kwargs) -> Any:
            self._wait_for_aloop_initialization()

            response = _WrappedResponse(threading.Event())

            def func_w_embedded_args() -> None:
                try:
                    response.result = func(self, *args, **kwargs)
                except BaseException as exc:
                    response.exception = exc
                response.done_event.set()

            self._aloop.call_soon_threadsafe(func_w_embedded_args)
            response.done_event.wait()

            if response.exception is not None:
                raise response.exception
            return response.result

        return wrapper

    def _threadsafe(self, func) -> Callable:
        no_result = object()

        def wrapper(*args, **kwargs):
            self._wait_for_aloop_initialization()

            response = {"result": no_result}
            done_event = threading.Event()

            def func_w_embedded_args():
                try:
                    response["result"] = func(*args, **kwargs)
                except Exception as exc:
                    response["exception"] = exc
                done_event.set()

            self._aloop.call_soon_threadsafe(func_w_embedded_args)
            done_event.wait()

            result = response.get("result")
            if result is no_result:
                raise response["exception"]
            return result

        return wrapper

    def stop_and_join(self) -> None:

        @self._threadsafe
        def request_stop():
            self._astop_event.set()

        if self.is_alive():
            request_stop()
            self.join()

    def _wait_for_aloop_initialization(self):
        if self.is_alive() and not self._astop_event.is_set():
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
        self.log({"event_type": "thread_starting"}, level=logging.DEBUG)

        self._aloop = asyncio.get_event_loop()
        self._astop_event = asyncio.Event()

        arun_task = asyncio.create_task(self._arun(), name="_arun")

        await self._ensure_service_is_running()

        await self._astop_event.wait()

        await arun_task

        self.log({"event_type": "thread_done"}, level=logging.DEBUG)

    async def _arun(self):
        pass
