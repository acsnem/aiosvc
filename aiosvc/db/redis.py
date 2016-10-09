import asyncio
import aioredis
from aiosvc import Componet


class Pool(Componet):

    def __init__(self, address: str = None, db=0, password=None, ssl=None, min_size: int = 10, max_size: int = 10,
                 start_priority=1, loop: asyncio.AbstractEventLoop = None, **connect_kwargs):
        super().__init__(loop=loop, start_priority=start_priority)
        self._address = address
        self._db = db
        self._password = password
        self._ssl = ssl
        self._min_size = min_size
        self._max_size = max_size
        self._connect_kwargs = connect_kwargs
        self._pool = None

    async def _start(self):
        self._pool = await aioredis.create_pool(loop=self._loop, address=self._address, db=self._db,
                                                minsize=self._min_size, maxsize=self._max_size,
                                                password=self._password, ssl=self._ssl, **self._connect_kwargs)

    async def _before_stop(self):
        pass

    async def _stop(self):
        self._pool.close()
        await self._pool.wait_closed()

    def acquire(self, timeout: float = None) -> aioredis.pool.RedisPool:
        """
        :param timeout: A timeout for acquiring a Connection.
        :type timeout: float | None
        :rtype: aioredis.pool.RedisPool
        """
        return PoolAcquireContext(self, timeout)


class PoolAcquireContext:

    __slots__ = ('timeout', 'connection', 'done', 'component')

    def __init__(self, component, timeout):
        self.component = component
        self.timeout = timeout
        self.connection = None
        self.done = False

    async def __aenter__(self):
        if self.connection is not None or self.done:
            raise UserWarning('a connection is already acquired')
        if self.timeout is None:
            self.connection = await self.component._pool.acquire()
        else:
            self.connection = await asyncio.wait_for(self.component._pool.acquire(),
                             timeout=self.timeout,
                             loop=self.component._loop)
        return self.connection

    async def __aexit__(self, *exc):
        self.done = True
        con = self.connection
        self.connection = None
        self.component._pool.release(con)

    # def __await__(self):
    #     self.done = True
    #     return self.component._pool.acquire(self.timeout).__await__()

