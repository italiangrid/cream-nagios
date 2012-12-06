
from mig.connection.abc import ConnABC
from mig.log import get_logger
log = get_logger()

try:
    from amqplib import client_0_8 as amqp
except ImportError:
    class amqp(object):
        Connection = object

class BrokerConn(ConnABC, amqp.Connection):
    def __init__(self, broker_uri='', **kwargs):
        log.error("BrokerConnAmqp class not implemented")
        raise NotImplementedError("BrokerConnAmqp class not implemented")
