import os
import configparser
import asyncio
import aiosvc.db.pg
import pytest
import aioredis
import aiosvc.db.redis

config = None


def setup_module(module):
    global config
    conf_file = os.path.dirname(__file__) + '/config.ini'
    config = configparser.ConfigParser()
    config.read(conf_file)


class TestPg:
    @pytest.mark.asyncio
    async def test_select(self, event_loop):
        dsn = config.get("pg", "dsn")

        db = aiosvc.db.pg.Pool(dsn, loop=event_loop)
        try:
            await db._start()

            async with db.acquire() as conn:
                assert await conn.fetchval("SELECT 1") == 1
        finally:
            await db._before_stop()
            await db._stop()

    @pytest.mark.asyncio
    async def test_pool(self, event_loop):
        dsn = config.get("pg", "dsn")
        timeout = config.getfloat('pg', 'timeout')

        db = aiosvc.db.pg.Pool(dsn, min_size=1, max_size=2, loop=event_loop)
        try:
            await db._start()

            async with db.acquire() as conn1:
                async with db.acquire() as conn2:
                    try:
                        async with db.acquire(timeout=timeout) as conn3:
                            success = False
                    except asyncio.TimeoutError as e:
                        success = True
            assert success == True

        finally:
            await db._before_stop()
            await db._stop()


class TestRedis:

    @pytest.mark.asyncio
    async def test_connect(self, event_loop):
        conn = await aioredis.create_connection(
            (config.get("redis", "host"), config.get("redis", "port")),
            loop=event_loop)
        key = 'my-test-key'
        val = b'qweasdzxc'
        await conn.execute('set', key, val)
        db_val = await conn.execute('get', key)
        assert val == db_val
        await conn.execute('del', key)
        db_val = await conn.execute('get', key)
        assert db_val is None
        conn.close()

    @pytest.mark.asyncio
    async def test_select(self, event_loop):
        db = aiosvc.db.redis.Pool(
            address=(config.get("redis", "host"), config.get("redis", "port")),
            loop=event_loop)
        try:
            await db._start()

            async with db.acquire() as conn:
                key = 'my-test-key2'
                val = b'1'
                await conn.connection.execute('set', key, val)
                db_val = await conn.connection.execute('get', key)
                assert val == db_val
                await conn.connection.execute('del', key)
        finally:
            await db._before_stop()
            await db._stop()

    @pytest.mark.asyncio
    async def test_pool(self, event_loop):
        timeout = config.getfloat('redis', 'timeout')
        db = aiosvc.db.redis.Pool(
            address=(config.get("redis", "host"), config.get("redis", "port")),
            loop=event_loop, min_size=1, max_size=2)
        try:
            await db._start()

            async with db.acquire() as conn1:
                async with db.acquire() as conn2:
                    try:
                        async with db.acquire(timeout=timeout) as conn3:
                            success = False
                    except asyncio.TimeoutError as e:
                        success = True
            assert success == True

            try:
                async with db.acquire(timeout=timeout) as conn1:
                    async with db.acquire(timeout=timeout) as conn2:
                        success = True
            except asyncio.TimeoutError as e:
                success = False
            assert success == True

        finally:
            await db._before_stop()
            await db._stop()
