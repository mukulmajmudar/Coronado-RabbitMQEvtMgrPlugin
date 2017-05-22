import json
import logging
import asyncio
import concurrent.futures

from Coronado.Plugin import AppPlugin as AppPluginBase
from tornado.platform.asyncio import AsyncIOMainLoop, to_tornado_future
from tornado.ioloop import IOLoop
import tornado.concurrent
import EventManagerPlugin

from .RabbitMQClient import RabbitMQClient

logger = logging.getLogger(__name__)

config = EventManagerPlugin.config
config.update(
{
    'rmqHost': 'localhost',
    'rmqPort': 5672,
    'rmqVirtualHost': '/',
    'rmqUsername': 'guest',
    'rmqPassword': 'guest',
    'rmqEnableSSL': False,
    'rmqSSLOptions': None
})

futureClasses = (tornado.concurrent.Future, asyncio.Future,
        concurrent.futures.Future)

class AppPlugin(EventManagerPlugin.AppPlugin):

    def getId(self):
        return 'rabbitmqEvtMgrPlugin'

    def start(self, context):
        # Install asyncio/tornado bridge if not already initialized
        if not IOLoop.initialized():
            AsyncIOMainLoop().install()

        if 'ioloop' not in context:
            context['ioloop'] = IOLoop.current()

        super().start(context)


    def makeEventManager(self):
        ioloop = self.context.get('ioloop', IOLoop.current())

        return EventManager(
                host=self.context['rmqHost'],
                port=self.context['rmqPort'],
                name=self.context['eventManagerName'],
                ioloop=ioloop)


class EventManager(EventManagerPlugin.EventManager):
    topicExName = None
    directExName = None
    client = None
    triggerCapable = None
    ioloop = None

    # pylint: disable=too-many-arguments
    def __init__(self, host, port, name, trigger=True, ioloop=None):
        # Call parent
        super().__init__(name)

        self.ioloop = ioloop is not None and ioloop or IOLoop.current()
        self.topicExName = self.name + '-topic'
        self.directExName = self.name + '-direct'
        self.triggerCapable = trigger

        # Create a client
        self.client = RabbitMQClient(host, port, self._onMessage, ioloop)


    async def start(self):
        if self.triggerCapable:
            # Declare direct and topic exchanges
            await self.client.declareExchange(self.topicExName, 'topic')
            await self.client.declareExchange(self.directExName, 'direct')


    async def on(self, eventType, listener, sourceId=None, listenerId=None):
        # Figure out the exchange and queue names based on whether
        # event type corresponds to a topic or direct exchange
        if sourceId is None:
            sourceId = self.name
        exchangeType = '.' in eventType and 'topic' or 'direct'
        exchangeName = '%s-%s' % (sourceId, exchangeType)
        if listenerId is None:
            logger.info('Automatic queue name for eventType %s', eventType)
            listenerId = ''
        queueName = listenerId

        # Declare exchange
        await self.client.declareExchange(exchangeName, exchangeType)

        # Declare queue
        queueResult = await self.client.declareQueue(queueName)

        # Bind the queue
        if queueName == '':
            queueName = queueResult
        await self.client.bindQueue(queueName, exchangeName, eventType)

        # Start consuming from the subscriber queue
        consumerTag = await self.client.startConsuming(queueName)

        # Associate message handler with consumer tag
        self._saveHandler(consumerTag, listener)

        return consumerTag


    async def trigger(self, eventType, **kwargs):
        contentType = kwargs.pop('contentType', 'application/json')
        contentEncoding = kwargs.pop('contentEncoding', 'utf-8')
        body = contentType == 'application/json' and \
                json.dumps(kwargs).encode(contentEncoding) or \
                kwargs['body']

        # If the key contains dots, publish to the topic exchange, otherwise
        # publish to the direct exchange
        exchangeName = '.' in eventType and self.topicExName \
                or self.directExName
        await self.client.publish(exchangeName, eventType, body,
                contentType, contentEncoding)


    async def off(self, listenerTag):
        # Stop consuming
        await self.client.stopConsuming(listenerTag)


    # pylint: disable=unused-argument
    def _onMessage(self, consumerTag, properties, body):
        # Get content type and encoding
        contentType, contentEncoding = properties.content_type, \
                properties.content_encoding

        result = None
        if contentType == 'application/json':
            kwargs = json.loads(body.decode(contentEncoding))

            # Call onEvent
            result = self._onEvent(consumerTag, **kwargs)
        else:
            result = self._onEvent(consumerTag, body=body,
                    contentType=contentType, contentEncoding=contentEncoding)

        # Handle asynchronous code if any
        if isinstance(result, futureClasses) or asyncio.iscoroutine(result):
            # Schedule coroutine, if any
            if asyncio.iscoroutine(result):
                result = asyncio.ensure_future(result)

            # Convert asyncio future to Tornado future
            if isinstance(result, asyncio.Future):
                result = to_tornado_future(result)

            self.ioloop.add_future(result, lambda f: f.result())
