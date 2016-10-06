import logging
import asyncio
import aiohttp.web
from aiosvc import Componet


logger = logging.getLogger("aiosvc")


class Server(Componet):

    def __init__(self, handlers, host='localhost', port=8888, stop_timeout=60.0, start_priority=5,
                 loop: asyncio.AbstractEventLoop = None):
        super().__init__(loop=loop, start_priority=start_priority)
        self._stop_timeout = stop_timeout
        self._host = host
        self._port = port
        self._initialized = False
        self._before_stopping = False
        self._stopping = False
        self._handler = None
        self._server = None
        self._handlers = handlers
        self._http_app = aiohttp.web.Application(logger=logger, loop=self._loop)

    async def _start(self):
        self._handler = self._http_app.make_handler()
        self._server = await self._loop.create_server(self._handler, self._host, self._port)

        for handler in self._handlers:
            await handler._setup(self._loop, self._app, self, self._http_app)

    async def _before_stop(self):
        self._server.close()
        await self._server.wait_closed()

    async def _stop(self):
        await self._http_app.shutdown()
        await self._handler.finish_connections(self._stop_timeout)
        await self._http_app.cleanup()
