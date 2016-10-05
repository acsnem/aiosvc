import asyncio
import asyncpg.pool
from aiosvc import Componet


class Pool(Componet):

    def __init__(self, loop: asyncio.AbstractEventLoop, dsn: str = None, min_size: int = 10, max_size: int = 10,
                 max_queries: int = 50000, setup=None, start_priority=1, **connect_kwargs):
        super().__init__(loop, start_priority)
        self._dsn = dsn
        self._min_size = min_size
        self._max_size = max_size
        self._max_queries = max_queries
        self._conn_setup = setup
        self._connect_kwargs = connect_kwargs
        self._pool = None

    async def _start(self):
        self._pool = await asyncpg.create_pool(loop=self._loop, dsn=self._dsn, min_size=self._min_size,
                                               max_size=self._max_size, max_queries=self._max_queries,
                                               setup=self._conn_setup, **self._connect_kwargs)

    async def _before_stop(self):
        pass

    async def _stop(self):
        await self._pool.close()

    def acquire(self, timeout: float = None) -> asyncpg.pool.Pool:
        """
        :param timeout: A timeout for acquiring a Connection.
        :type timeout: float | None
        :rtype: asyncpg.pool.Pool
        """
        return self._pool.acquire(timeout=timeout)
