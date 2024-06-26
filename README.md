# aioservicethread

An abstract class for asyncio services that run in their dedicated threads.

## Installation

```shell
pip install aioservicethread
```

## Examples

### Running multiple services

This example shows running multiple services in dedicated threads. When the server needs to shut down, each service can execute its graceful shutdown procedures.

```python
import asyncio
import logging
import time

from aioservicethread import AioServiceThread

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ...


class MyServer(AioServiceThread):
    port: int

    def __init__(self, name: str = "server-1", port: int = 8080):
        super().__init__(name=name)
        self.port = port

    async def serve_forever(self):
        while True:
            await asyncio.sleep(3600)

    async def _arun(self):
        self.service_running.set()
        self.log(f"started on port {self.port}")

        # start serving
        task1 = asyncio.create_task(self.serve_forever())

        # wait for the stop event
        await self._astop_event.wait()

        # gracefully shut down the server
        self.log("stopping")

        task1.cancel()
        try:
            await task1
        except asyncio.CancelledError:
            pass

        self.log("done")


# ...


def main():
    server1 = MyServer("server-1", port=8080)
    server2 = MyServer("server-2", port=8181)

    logger.info("starting services ...")

    server1.start()
    server2.start()

    # serve until stopped
    print("Press Ctrl-C to exit")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print(" shutting down")

    logger.info("stopping services")

    server1.stop_and_join()
    server2.stop_and_join()

    logger.info("done")


if __name__ == "__main__":
    main()
```

Output:
```log
INFO:__main__:starting services ...
INFO:MyServer.server-2:started on port 8181
Press Ctrl-C to exit
INFO:MyServer.server-1:started on port 8080
^C shutting down
INFO:__main__:stopping services
INFO:MyServer.server-1:stopping
INFO:MyServer.server-1:done
INFO:MyServer.server-2:stopping
INFO:MyServer.server-2:done
INFO:__main__:done
```
