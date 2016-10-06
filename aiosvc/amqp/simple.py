import uuid
import logging
import asyncio

import aioamqp
import aioamqp.protocol
import aioamqp.channel

import aiosvc


logger = logging.getLogger("amqp")
aioamqp.protocol.logger = logger
aioamqp.channel.logger = logger


class Connection(aiosvc.Componet):

    def __init__(self, url='amqp://guest:guest@localhost:5672/', heartbeat=1, reconnect_timeout=1, declare=None,
                 start_priority=2, loop: asyncio.AbstractEventLoop = None):
        """

        :param loop: asyncio.AbstractEventLoop
        :param url: str
        :param heartbeat: int
        :param reconnect_timeout: int

        Examples:
            declare={
                "exchanges": [
                    {"name": "exchange_1", "type": "topic"}
                ],
                "queues": [
                    {"name": "queue_1", "exclusive": True}
                ],
                "bindings": [
                    {"exchange": "exchange_1", "queue": "queue_1", "routing_key": "*.*"}
                ],
            }
        """
        super().__init__(loop=loop, start_priority=start_priority)
        self._url = url
        self._heartbeat = heartbeat
        self._declare = declare
        self._reconnect_timeout = reconnect_timeout
        self._transport = None
        self._protocol = None
        self._channel = None
        self._connecting = False
        self._stopping = False
        self._stopped = True

    async def _start(self):
        logger.info("Start")
        if self._connecting or self._protocol is not None:
            return False

        self._connecting = True
        self._stopping = self._stopped = False
        try:
            try:
                self._transport, self._protocol = await aioamqp.from_url(self._url, loop=self._loop,
                                                                         heartbeat=self._heartbeat,
                                                                         on_error=self.on_error)
            except (aioamqp.AmqpClosedConnection, ConnectionError) as e:
                self._loop.create_task(self.on_error(e))
                return False

            logger.info("Connection to amqp server established")

            self._channel = await self._protocol.channel()
            print(self._channel)

            if self._declare is not None:
                await self._declare_all()
        finally:
            self._connecting = False
        return True

    async def _before_stop(self):
        pass

    async def _stop(self):
        logger.info("Stop")
        # self._stopping = True

        if self._channel is not None and self._channel.is_open:
            try:
                await self._channel.close()
            except Exception as e:
                logger.exception(e)
        self._channel = None

        if self._protocol is not None:
            try:
                await self._protocol.close()
            except Exception as e:
                logger.exception(e)
        self._protocol = None

        self._stopping = False
        self._stopped = True

    async def on_error(self, error):
        logger.exception(error)

        if self._channel is not None and self._channel.is_open:
            ch = self._channel
            self._channel = None
            try:
                await ch.close()
            except Exception as e:
                logger.exception(e)

        # fix infinity loop on disconnect
        try:
            if self._protocol and not self._protocol.stop_now.done():
                self._protocol.stop()
        except Exception as e:
            logger.exception(e)

        try:
            if self._protocol:
                await self._protocol.close()
        except Exception as e:
            logger.exception(e)
            # self._loop.stop()
        self._protocol = None

        while not self._stopping:
            logger.info("Trying to restore the connection with amqp server")
            await asyncio.sleep(self._reconnect_timeout, loop=self._loop)
            try:
                await self._start()
                # if self._consuming:
                #     await self.consume()
                break
            except Exception as e:
                logger.exception(e)

    async def _declare_all(self):
        if "exchanges" in self._declare:
            for settings in self._declare["exchanges"]:
                logger.info("Declaring exchange: %s" % settings["name"])
                await self._channel.exchange_declare(exchange_name=settings["name"],
                                               type_name=settings.get("type", "direct"),
                                               passive=settings.get("passive", False),
                                               durable=settings.get("durable", True),
                                               auto_delete=settings.get("auto_delete", False),
                                               no_wait=settings.get("no_wait", False),
                                               arguments=settings.get("arguments", None),
                                               timeout=settings.get("timeout", None))

        if "queues" in self._declare:
            for settings in self._declare["queues"]:
                logger.info("Declaring queue: %s" % settings["name"])
                await self._channel.queue_declare(queue_name=settings["name"],
                                            passive=settings.get("passive", False),
                                            durable=settings.get("durable", True),
                                            exclusive=settings.get("exclusive", False),
                                            auto_delete=settings.get("auto_delete", False),
                                            no_wait=settings.get("no_wait", False),
                                            arguments=settings.get("arguments", None),
                                            timeout=settings.get("timeout", None))

        if "bindings" in self._declare:
            for settings in self._declare["bindings"]:
                logger.info('Binding exchange "%s" to queue "%s" with routing key "%s"' % (settings["exchange"], settings["queue"], settings["routing_key"]))
                await self._channel.queue_bind(queue_name=settings["queue"],
                                         exchange_name=settings["exchange"],
                                         routing_key=settings["routing_key"],
                                         no_wait=settings.get("no_wait", False),
                                         arguments=settings.get("arguments", None),
                                         timeout=settings.get("timeout", None))


class Publisher(Connection):

    def __init__(self, exchange, publish_timeout=5, try_publish_interval=.9, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._exchange_name = exchange
        self._publish_timeout = publish_timeout
        self._try_publish_interval = try_publish_interval
        self._consumer_tag = None
        self._publishing = 0

    async def _stop(self):
        self._stopping = True
        # TODO сделать без sleep
        while self._publishing > 0:
            await asyncio.sleep(.1, loop=self._loop)
        await super()._stop()

    async def publish(self, payload, routing_key='', properties=None, mandatory=False, immediate=False):
        logger.info("Publishing message: %s" % payload)
        if self._stopping or self._stopped:
            logger.error("Attempt to pusblish message when server is stopping. Payload: %s" % payload )
            raise RuntimeError("It is impossible to send a message during the shutdown server")

        self._publishing += 1
        try:
            await asyncio.wait_for(self._try_publish(payload, routing_key, properties, mandatory, immediate),
                           timeout=self._publish_timeout, loop=self._loop)
        except Exception as e:
            logger.error("Message has not been sent to amqp server. Reason: [%s] %s. Payload: %s" % (
                str(type(e)), str(e), payload))
        finally:
            self._publishing -= 1

    async def _try_publish(self, payload, routing_key='', properties=None, mandatory=False, immediate=False):
        while not self._stopping and not self._stopped:
            try:
                if self._channel:
                    logger.info("Channel publishing message: %s" % payload)
                    await self._channel.basic_publish(payload, self._exchange_name, routing_key,
                                                      properties=properties, mandatory=mandatory, immediate=immediate)
                    logger.info("Message published: %s" % payload)
                    break
                else:
                    logger.warn("Message can not be sent until there is no connection to the server")
                    await asyncio.sleep(self._try_publish_interval, loop=self._loop)
            except aioamqp.exceptions.ChannelClosed as e:
                print(type(e), e)
                print('sleep publisher')
                await asyncio.sleep(self._try_publish_interval, loop=self._loop)


class Consumer(Connection):

    def __init__(self, queue=None, prefetch_count=1, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._queue_name = queue or str(uuid.uuid4())
        self._prefetch_count = prefetch_count
        self._consumer_tag = None
        self._last_delivery_tag = None

    async def _start(self):
        if await super()._start():
            await self._consume()

    async def _before_stop(self):
        logger.info("Prepare for stop consumer")
        await super()._before_stop()
        self._stopping = True
        if self._consumer_tag and self._channel:
            try:
                logger.info('Stop consuming queue "%s"' % self._queue_name)
                await self._channel.basic_cancel(self._consumer_tag, no_wait=False, timeout=None)
                self._consumer_tag = None
            except Exception as e:
                logger.exception(e)

    async def _stop(self):
        logger.info("Stop consumer")
        await super()._stop()

    async def _consume(self):
        logger.info('Prepare for consuming')
        if self._prefetch_count is not None:
            logger.info("Setting prefetch_count: %s" % self._prefetch_count)
            await self._channel.basic_qos(prefetch_count=self._prefetch_count, prefetch_size=0, connection_global=False)

        self._consumer_tag = 'ctag%i.%s' % (self._channel.channel_id, uuid.uuid4().hex)
        logger.info("Start consuming queue: %s [%s]" % (self._queue_name, self._consumer_tag))
        await self._channel.basic_consume(self._callback, queue_name=self._queue_name, consumer_tag=self._consumer_tag)

    async def _callback(self, channel, body, envelope, properties):
        logger.info("Received message: %s" % (body, ))
        self._last_delivery_tag = envelope.delivery_tag
        await self.handle(body, envelope, properties)

    async def ack_last(self):
        if self._prefetch_count != 1:
            raise UserWarning('Do not use function ack_last() with prefetch_count != 1')
        logger.info('Ack message')
        await self._channel.basic_client_ack(delivery_tag=self._last_delivery_tag)

    async def handle(self, body, envelope, properties):
        # await channel.basic_client_ack(delivery_tag=envelope.delivery_tag)
        raise NotImplementedError()

