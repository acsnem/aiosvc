import asyncio
import aiosvc
from aiohttp.web import Response


class SimpleHandler:
    """
    :type _app: aiosvc.Application
    :type _loop: asyncio.AbstractEventLoop
    """

    def __init__(self, route, methods=None):
        self._route = route
        self._app = None
        self._loop = None
        self._server = None
        self._http_app = None
        self._methods = methods or ["GET"]

    async def _setup(self, loop, app, server, http_app):
        self._loop = loop
        self._app = app
        self._server = server
        self._http_app = http_app
        await self._add_routes()

    async def _add_routes(self):
        for method in self._methods:
            self._http_app.router.add_route(method, self._route, self._handle_request)

    @property
    def app(self):
        """
        :return: aiosvc.Application
        :rtype: aiosvc.Application
        """
        return self._app

    async def _handle_request(self, request):
        return await self.handle(request)

    async def handle(self, request):
        # await asyncio.sleep(10)
        return Response(text='OK')
