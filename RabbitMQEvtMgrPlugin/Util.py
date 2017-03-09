import functools

import tornado.concurrent
from tornado.ioloop import IOLoop

def when(*args, **kwargs):
    '''
    Deprecated.

    A JQuery-like "when" function to gather futures and deal with
    maybe-futures. Copied from previous version of Coronado.
    '''
    # If ioloop not given, use the current one
    ioloop = kwargs.get('ioloop', IOLoop.current())

    makeTuple = kwargs.get('makeTuple', False)

    future = tornado.concurrent.Future()
    numDone = [0]
    numFutures = len(args)
    result = [None] * numFutures

    def onFutureDone(index, f):
        result[index] = f
        numDone[0] += 1
        if numDone[0] == numFutures:
            if numFutures > 1 or makeTuple:
                future.set_result(tuple(result))
            else:
                tornado.concurrent.chain_future(f, future)

    index = 0
    for maybeFuture in args:
        if isinstance(maybeFuture, tornado.concurrent.Future):
            ioloop.add_future(maybeFuture,
                    functools.partial(onFutureDone, index))
        elif isinstance(maybeFuture, Exception):
            # Make a future with the exception set to the argument
            f = tornado.concurrent.Future()
            f.set_exception(maybeFuture)
            onFutureDone(index, f)
        else:
            # Make a future with the result set to the argument
            f = tornado.concurrent.Future()
            f.set_result(maybeFuture)
            onFutureDone(index, f)
        index += 1

    if numFutures == 0:
        future.set_result(tuple())

    return future


def transform(future, callback, ioloop=None):
    '''
    Deprecated.

    A future transformer. Similar to JQuery's "deferred.then()". Copied
    from previous version of Coronado.
    '''
    if ioloop is None:
        ioloop = IOLoop.current()
    newFuture = tornado.concurrent.Future()

    def onFutureDone(future):
        try:
            transformedValue = callback(future)
        except Exception as e:  # pylint: disable=broad-except
            transformedValue = e
        nextFuture = when(transformedValue)
        tornado.concurrent.chain_future(nextFuture, newFuture)

    ioloop.add_future(future, onFutureDone)
    return newFuture
