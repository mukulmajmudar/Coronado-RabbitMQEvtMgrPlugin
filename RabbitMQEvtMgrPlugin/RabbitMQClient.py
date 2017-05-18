import logging
from functools import wraps
import asyncio

import pika
from pika.adapters import TornadoConnection
from pika.spec import BasicProperties
from tornado.ioloop import IOLoop

# Logger for this module
logger = logging.getLogger(__name__)


def connected(method):
    '''
    Decorator to ensure that the Client is connected to the RabbitMQ server.
    '''

    @wraps(method)
    async def wrapper(self, *args, **kwargs):
        # If already connected, call method immediately
        if self._connected:
            result = method(self, *args, **kwargs)
            if asyncio.iscoroutine(result):
                return await result
            else:
                return result
        else:
            # Not connected, so connect and then call method
            await self.connect()

            # Call actual method
            result = method(self, *args, **kwargs)
            if asyncio.iscoroutine(result):
                return await result
            else:
                return result

    return wrapper


class RabbitMQClient(object):
    '''
    A RabbitMQ client.

    This client use pika internally but returns futures for asynchronous
    operations.
    '''

    connectAttempt = None

    def __init__(self, host, port, messageHandler=None, ioloop=None):
        self._host = host
        self._port = port
        self._messageHandler = messageHandler
        self._ioloop = ioloop is not None and ioloop or IOLoop.current()
        self._connected = False
        self.connectAttempt = 0


    @connected
    async def declareExchange(self, name, exchangeType):
        declareFuture = asyncio.Future()
        def onExchangeDeclared(frame):  # pylint: disable=unused-argument
            logger.info('Declared RabbitMQ exchange %s of type %s',
                    name, exchangeType)
            declareFuture.set_result(None)

        logger.info('Declaring RabbitMQ exchange %s of type %s',
                name, exchangeType)
        self._channel.exchange_declare(
                callback=onExchangeDeclared,
                exchange=name,
                exchange_type=exchangeType,
                durable=True)

        return await declareFuture


    @connected
    async def declareQueue(self, name):
        declareFuture = asyncio.Future()

        def onQueueDeclared(methodFrame):   # pylint: disable=unused-argument
            queueName = name
            if queueName == '':
                # Get auto-generated queue name
                queueName = methodFrame.method.queue
            logger.info('Declared RabbitMQ queue %s', queueName)
            declareFuture.set_result(queueName)

        logger.info('Declaring RabbitMQ queue %s', name)

        # If no queue name, declare a temporary queue
        if name == '':
            self._channel.queue_declare(onQueueDeclared, auto_delete=True,
                    exclusive=True)
        else:
            # Declare a durable queue
            self._channel.queue_declare(onQueueDeclared, name, durable=True)

        return await declareFuture


    @connected
    async def bindQueue(self, queueName, exchangeName, key):
        bindFuture = asyncio.Future()

        def onQueueBound(frame):    # pylint: disable=unused-argument
            logger.info('Bound queue %s to exchange %s',
                    queueName, exchangeName)
            bindFuture.set_result(None)

        logger.info('Binding queue %s to exchange %s',
                queueName, exchangeName)
        self._channel.queue_bind(onQueueBound, queueName, exchangeName,
                key)

        return await bindFuture


    async def connect(self):
        logger.info('Connecting to RabbitMQ server')

        if self._connectFuture is not None:
            return self._connectFuture

        # Make connection
        self._connectFuture = asyncio.Future()
        params = pika.ConnectionParameters(host=self._host, port=self._port)
        self._connection = TornadoConnection(params,
                on_open_callback=self._onConnected,
                on_open_error_callback=self._onConnectError,
                on_close_callback=self._onConnectionClosed,
                custom_ioloop=self._ioloop)

        return await self._connectFuture


    async def disconnect(self):
        if not self._connected:
            return
        self._disconnectFuture = asyncio.Future()
        self._connection.close()
        return await self._disconnectFuture


    @connected
    # pylint: disable=too-many-arguments
    def publish(self, exchangeName, routingKey, body,
            contentType, contentEncoding):
        # Define properties
        properties = BasicProperties(
                content_type=contentType,
                content_encoding=contentEncoding,
                delivery_mode=2)

        # Publish to RabbitMQ server
        self._channel.basic_publish(exchange=exchangeName,
                routing_key=routingKey, body=body,
                properties=properties)


    @connected
    def startConsuming(self, queueName):
        # Add on-cancel callback
        def onCancel(frame):    # pylint: disable=unused-argument
            self._channel.close()
        self._channel.add_on_cancel_callback(onCancel)

        # Start consuming
        consumerTag = self._channel.basic_consume(
                self._onMessage, queueName)
        logger.info('Started consuming from queue %s', queueName)

        return consumerTag


    async def stopConsuming(self, consumerTag):
        logger.info('Stopping RabbitMQ consumer')
        stopFuture = asyncio.Future()
        def onCanceled(unused): # pylint: disable=unused-argument
            logger.info('Canceled RabbitMQ consumer')
            stopFuture.set_result(None)
        self._channel.basic_cancel(onCanceled, consumerTag)
        return await stopFuture


    def _onConnected(self, connection):
        # Open a channel in the connection
        self._channel = connection.channel(self._onChannel)
        self.connectAttempt = 0


    # pylint: disable=unused-argument
    def _onConnectError(self, *args, **kwargs):
        if self.connectAttempt == 10:
            logger.info('Could not connect to RabbitMQ, will not try ' +
                'again.')
            self._connectFuture.set_exception(ConnectionError())
            self._connectFuture = None
        else:
            # Assume RabbitMQ is not up yet, sleep
            logger.info('Could not connect to RabbitMQ, will try ' +
                'again after 10 seconds...')
            self.connectAttempt += 1
            self._ioloop.call_later(5, self.connect)


    # pylint: disable=unused-argument
    def _onConnectionClosed(self, connection, replyCode, replyText):
        logger.info('RabbitMQ server connection closed')
        self._connected = False
        if self._disconnectFuture is not None:
            self._disconnectFuture.set_result(None)
            self._disconnectFuture = None


    def _onChannel(self, channel):
        # Add channel-close callback
        channel.add_on_close_callback(self._onChannelClosed)
        self._connected = True
        logger.info('Connected to RabbitMQ server')
        self._connectFuture.set_result(None)
        self._connectFuture = None


    # pylint: disable=unused-argument
    def _onChannelClosed(self, channel, replyCode, replyText):
        self._connected = False
        logger.info('RabbitMQ channel closed')
        self._connection.close()


    def _onMessage(self, channel, basicDeliver, properties, body):
        #logger.info('Message received (may be partial): %s', body[0:50])
        #logger.debug('Message body (may be partial): %s', body[0:1000])
        self._messageHandler(basicDeliver.consumer_tag, properties, body)

        # Acknowledge message
        self._channel.basic_ack(basicDeliver.delivery_tag)


    _host = None
    _port = None
    _messageHandler = None
    _ioloop = None
    _connected = None
    _connectFuture = None
    _connection = None
    _channel = None
    _disconnectFuture = None
