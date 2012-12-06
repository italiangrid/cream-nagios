
import stomp

from mig import utils

from mig.connection.abc import ConnABC
from msgdirq import Message

from mig.log import get_logger
log = get_logger()
# TODO: get rid of this
import logging

class MyListener(object):
    def __init__(self, verb=''):
        self.is_connected = False
        self.errors = 0
        self.connections = 0
        self.messages_recd = 0
        self.receipts = 0
        self.messages_sent = 0
        if verb:
            log.setLevel(logging._levelNames[verb.upper()])

    def on_connecting(self, host_and_port):
        self.connections += 1
        log.info('Connecting : %s:%s' % host_and_port)

    def on_connected(self, headers, body):
        self.connections += 1
        self.is_connected = True
        self.__print_async("CONNECTED", headers, body)

    def on_disconnected(self, headers, body):
        self.is_connected = False
        self.__print_async("LOST CONNECTION", headers, body)

    def on_send(self, headers, body):
        self.messages_sent += 1
        self.__print_async("Sending MESSAGE", headers, body, level='debug')

    def on_message(self, headers, body):
        self.messages_recd += 1
        self.__print_async("Got MESSAGE", headers, body, level='debug')

    def on_error(self, headers, body):
        self.errors += 1
        self.__print_async("ERROR", headers, body, level='error')

    def on_receipt(self, headers, body):
        self.receipts += 1
        self.__print_async("RECEIPT", headers, body, level='debug')

    def __print_async(self, opframe, headers, body, level='info'):
        loglvl = getattr(log, level)
        loglvl('%s'%opframe)
        if headers:
            loglvl('headers : %s' % str(headers))
        if body:
            loglvl("body : '%s'" % body)

    def __str__(self):
        return '''
Connections: %i
Messages sent: %i
Receipts received: %i
Messages received: %i
Errors: %i''' % (self.connections, self.messages_sent, self.receipts,
                 self.messages_recd, self.errors)

class BrokerConn(ConnABC, stomp.Connection):
    def __init__(self, broker_uri='', **kwargs):
        try:
            verb = kwargs['verbosity']
            del kwargs['verbosity']
        except KeyError:
            verb = False
        host, port = utils.parse_uri2(broker_uri)
        stomp.Connection.__init__(self, [(host, int(port))],
                                  kwargs)
        self.listener = MyListener(verb)
        self.set_listener('MyListener', self.listener)
        self.start()
        self.connect()
    def send(self, m):
        if not isinstance(m, Message):
            raise TypeError("Expected instance of Message, got %s" % type(m))
        if isinstance(m.body, unicode):
            stomp.Connection.send(self, m.body.encode('ascii','ignore'),
                                  m.header)
        else:
            stomp.Connection.send(self, str(unicode(m.body,errors='ignore')),
                                  m.header)
    def stop(self):
        log.info("Statistics:%s" % self.listener)
        stomp.Connection.stop(self)
    def disconnect(self):
        log.info("Statistics:%s" % self.listener)
        stomp.Connection.disconnect(self)
