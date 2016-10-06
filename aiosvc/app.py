import logging
import asyncio
from collections import OrderedDict


logger = logging.getLogger("aiosvc")


class Componet(object):
    """
    :type _loop: asyncio.AbstractEventLoop
    :type _app: Application
    """

    def __init__(self, *, loop=None, start_priority=1):
        self._loop = loop
        self._app = None
        self._start_priority = start_priority

    @property
    def app(self):
        """
        :return: aiosvc.Application
        :rtype: aiosvc.Application
        """
        return self._app

    async def _setup(self, app):
        """
        :type app: Application
        """
        if not isinstance(app, Application):
            raise UserWarning("App must me instance of %s.Application" % (__package__, ))
        self._app = app

    async def _start(self):
        raise NotImplementedError

    async def _before_stop(self):
        raise NotImplementedError

    async def _stop(self):
        raise NotImplementedError


class Application(object):

    def __init__(self, loop: asyncio.AbstractEventLoop=None):
        self._loop = loop or asyncio.get_event_loop()
        self._components = OrderedDict()

    def attach(self, name: str, component: Componet):
        if not isinstance(component, Componet):
            raise UserWarning('Component "%s" must be instance of %s.Componet' % (__package__, name))
        if name in self._components:
            raise UserWarning('Component "%s" already attached' % (name, ))
        self._components[name] = component
        if component._loop is None:
            component._loop = self._loop
        self._loop.run_until_complete(component._setup(self))

    def __getattr__(self, item: str) -> Componet:
        if item not in self._components:
            raise AttributeError('Component "%s" doesn\'t attached' % item)
        return self._components[item]

    def run(self):
        start_order = sorted(self._components.items(), key=lambda x: x[1]._start_priority)
        for name, component in start_order:
            print(name)
            self._loop.run_until_complete(component._start())
        try:
            self._loop.run_forever()
        except KeyboardInterrupt:  # pragma: no branch
            pass
        finally:
            logger.info("Prepare to stop application")
            for name, component in reversed(start_order):
                self._loop.run_until_complete(component._before_stop())
            logger.info("Stopping application")
            for name, component in reversed(start_order):
                self._loop.run_until_complete(component._stop())
            logger.info('Loop close')
            self._loop.close()
