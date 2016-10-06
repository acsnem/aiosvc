#!/usr/bin/python3

import logging
import asyncio
import aiosvc
import aiosvc.web
import aiosvc.db
import aiosvc.amqp


class Application(aiosvc.Application):

    pass


class JsonRpcHandler(aiosvc.web.server.rpc.JsonRpcHandler):

    async def test(self, a=1):
        try:
            async with self.app.db.acquire() as conn:
                res = await conn.fetchval("SELECT random()")
        except Exception as eee:
            print('ERROR', eee)

        await self.app.apub.publish('test')
        # await asyncio.sleep(a)
        return "OK: %s" % res

class RestRpcHandler(aiosvc.web.server.rpc.RestRpcHandler):

    async def get_test(self, a):
        return a

    async def tt_ree(self):
        return 1


class Consumer(aiosvc.amqp.Consumer):

    async def handle(self, body, envelope, properties):
        print("### NEW MESSAGE ###", body)
        await self.ack_last()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    loop = asyncio.get_event_loop()

    config = {
        "amqp_consumer": {
            "server": {
                "url": "localhost",
            }
        }
    }

    amqp_declare = {
        "exchanges": [
            {"name": "exchange_1", "type": "topic"},
            {"name": "exchange_2", "type": "direct"},
        ],
        "queues": [
            {"name": "queue_1"},
            {"name": "queue_2"},
        ],
        "bindings": [
            {"exchange": "exchange_1", "queue": "queue_1", "routing_key": "#"},
            {"exchange": "exchange_2", "queue": "queue_2", "routing_key": ""},
        ],
    }
    # amqp_declare=None

    rpc_listening = ('127.0.0.1', 8888)

    app = Application(loop)
    app.attach(
        'web',
        aiosvc.web.server.Server(
            # loop=loop,
            host='localhost',
            port=8888,
            handlers=[
                aiosvc.web.server.SimpleHandler(
                    route="/",
                    methods=["GET", "POST"]
                ),
                JsonRpcHandler(
                    route="/jsonrpc/",
                ),
                RestRpcHandler(
                    route='/restrpc/'
                )
            ]
        )
    )
    app.attach(
        'apub',
        aiosvc.amqp.Publisher(
            # loop=loop,
            exchange='exchange_1'
        )
    )
    app.attach(
        'acons',
        Consumer(
            # loop=loop,
            queue='queue_1'

        )
    )
    # app.attach(
    #     'jsonrpc',
    #
    # )
    # app.attach(
    #     'restrpc',
    #     RestRpcHandler(
    #         loop=loop,
    #         host=rpc_listening[0],
    #         port=rpc_listening[1],
    #         route="/restrpc",
    #     )
    # )
    # app.attach(
    #     'amqppublisher',
    #     aiosvc.amqp.Publisher(
    #         loop=loop,
    #         exchange="exchange_1",
    #         declare=amqp_declare
    #     )
    # )
    # app.attach(
    #     'amqpconsumer',
    #     AmqpConsumer(
    #         loop=loop,
    #         queue="queue_1",
    #         declare=amqp_declare
    #     )
    # )
    #
    # app.attach(
    #     'taskmanager',
    #     AmqpTaskManager(
    #         loop=loop,
    #         queue="queue_2",
    #         exchange="exchange_2",
    #         declare=amqp_declare
    #     )
    # )
    app.attach(
        'db',
        aiosvc.db.PgPool(
            # loop=loop,
            dsn="postgresql://postgres:postgres@localhost/ars",
            max_size=10,
            min_size=2
        )
    )
    app.run()
