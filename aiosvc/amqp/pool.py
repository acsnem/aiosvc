import logging
import asyncio

from aiosvc import Componet
from .simple import Publisher


# class Pool(Componet):
#
#     def __init__(self, exchange, *, publish_timeout=5, try_publish_interval=.9, size=1, max_size=2, loop=None, start_priority=1):
#         super().__init__(loop=loop, start_priority=start_priority)
#         # Publisher(exchange=exchange, publish_timeout=publish_timeout, try_publish_interval=try_publish_interval)

class Pool(Componet):
    """A connection pool.
    Connection pool can be used to manage a set of connections to the AMQP server.
    Connections are first acquired from the pool, then used, and then released
    back to the pool.  Once a connection is released, it's reset to close all
    open cursors and other resources *except* prepared statements.
    Pools are created by calling :func:`~asyncpg.pool.create_pool`.
    """

    # __slots__ = ('_queue', '_loop', '_minsize', '_maxsize',
    #              '_connect_args', '_connect_kwargs',
    #              '_working_addr', '_working_opts',
    #              '_con_count', '_max_queries', '_connections',
    #              '_initialized', '_closed', '_setup')

    def __init__(self,
                 exchange,
                 min_size,
                 max_size,
                 publish_timeout=5,
                 try_publish_interval=.9,
                 loop=None,
                 start_priority=1):
        super().__init__(loop=loop, start_priority=start_priority)
        self._exchange = exchange
        self._publish_timeout = publish_timeout
        self._try_publish_interval = try_publish_interval

        if max_size <= 0:
            raise ValueError('max_size is expected to be greater than zero')

        if min_size <= 0:
            raise ValueError('min_size is expected to be greater than zero')

        if min_size > max_size:
            raise ValueError('min_size is greater than max_size')

        self._minsize = min_size
        self._maxsize = max_size

        self._reset()

        self._closed = False

    async def _start(self):
        await self._init()

    async def _before_stop(self):
        await asyncio.gather(*[con._before_stop() for con in self._connections], loop=self._loop)

    async def _stop(self):
        await asyncio.gather(*[con._stop() for con in self._connections], loop=self._loop)

    async def _new_connection(self):
        con = Publisher(self._exchange, publish_timeout=self._publish_timeout,
                        try_publish_interval=self._try_publish_interval, loop=self._loop)
        try:
            await con._start()
        except Exception as e:
            logging.exception(e)
            try:
                await con._before_stop()
                await con._stop()
            except:
                pass

        self._connections.add(con)
        return con

    async def _init(self):
        if self._initialized:
            return
        if self._closed:
            raise Exception('pool is closed')

        for _ in range(self._minsize):
            self._con_count += 1
            try:
                con = await self._new_connection()
            except:
                self._con_count -= 1
                raise
            self._queue.put_nowait(con)

        self._initialized = True
        return self

    def acquire(self, *, timeout=None):
        """Acquire a AMQP connection from the pool.
        :param float timeout: A timeout for acquiring a Connection.
        :return: An instance of :class:`~asyncpg.connection.Connection`.
        Can be used in an ``await`` expression or with an ``async with`` block.
        .. code-block:: python
            async with pool.acquire() as con:
                await con.execute(...)
        Or:
        .. code-block:: python
            con = await pool.acquire()
            try:
                await con.execute(...)
            finally:
                await pool.release(con)
        """
        return PoolAcquireContext(self, timeout)

    async def _acquire(self, timeout):
        if timeout is None:
            return await self._acquire_impl()
        else:
            return await asyncio.wait_for(self._acquire_impl(),
                                          timeout=timeout,
                                          loop=self._loop)

    async def _acquire_impl(self):
        self._check_init()

        try:
            con = self._queue.get_nowait()
        except asyncio.QueueEmpty:
            con = None

        if con is None:
            if self._con_count < self._maxsize:
                self._con_count += 1
                try:
                    con = await self._new_connection()
                except:
                    self._con_count -= 1
                    raise
            else:
                con = await self._queue.get()

        return con

    async def release(self, connection):
        """Release a AMQP connection back to the pool."""
        self._check_init()
        # if connection.is_closed():
        #     self._con_count -= 1
        #     self._connections.remove(connection)
        # else:
        # await connection.reset()
        self._queue.put_nowait(connection)

    async def close(self):
        """Gracefully close all connections in the pool."""
        if self._closed:
            return
        self._check_init()
        self._closed = True
        coros = []
        for con in self._connections:
            coros.append(con._before_stop())
        await asyncio.gather(*coros, loop=self._loop)
        coros = []
        for con in self._connections:
            coros.append(con._stop())
        await asyncio.gather(*coros, loop=self._loop)
        self._reset()

    def _check_init(self):
        if not self._initialized:
            raise Exception('pool is not initialized')
        if self._closed:
            raise Exception('pool is closed')

    def _reset(self):
        self._connections = set()
        self._con_count = 0
        self._initialized = False
        self._queue = asyncio.Queue(maxsize=self._maxsize, loop=self._loop)

    def __await__(self):
        return self._init().__await__()

    async def __aenter__(self):
        await self._init()
        return self

    async def __aexit__(self, *exc):
        await self.close()


class PoolAcquireContext:

    __slots__ = ('timeout', 'connection', 'done', 'pool')

    def __init__(self, pool, timeout):
        self.pool = pool
        self.timeout = timeout
        self.connection = None
        self.done = False

    async def __aenter__(self):
        if self.connection is not None or self.done:
            raise Exception('a connection is already acquired')
        self.connection = await self.pool._acquire(self.timeout)
        return self.connection

    async def __aexit__(self, *exc):
        self.done = True
        con = self.connection
        self.connection = None
        await self.pool.release(con)

    def __await__(self):
        self.done = True
        return self.pool._acquire(self.timeout).__await__()