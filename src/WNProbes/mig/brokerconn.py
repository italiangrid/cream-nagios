
import sys

from mig import utils

class BrokerConnFactory(object):
    """Instantiate a broker connector based on a protocol in broker URI.
    """
    def __new__(cls, broker_uri, **kwargs):
        proto = utils.parse_uri3(broker_uri)[0]
        if proto in ['stomp','stomp+ssl']:
            from mig.connection.migstomp import BrokerConn
        elif proto in ['amqp']:
            from mig.connection.migamqp import BrokerConn
        else:
            raise TypeError("No broker connector for %s." % proto)
        return BrokerConn(broker_uri, **kwargs)
