import os
import configparser
import asyncio
import aiosvc.db.pg
import pytest


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

