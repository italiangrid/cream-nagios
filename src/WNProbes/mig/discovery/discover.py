
from mig.discovery.ldaputils import ldapsearch
from mig.discovery.fileutils import filesearch

DEFAULT_BROKER_NETWORK = 'PROD'

def discover_brokers(discovery_endp, network=DEFAULT_BROKER_NETWORK):
    """Return broker endpoint from specified network.
    discovery_endp - supported discovery schemes ldap:, [file:|/]
    network - brokers network to pick brokers from.
    """
    if discovery_endp.startswith('/') or discovery_endp.startswith('file:'):
        return _discover_brokers_file(discovery_endp, network)
    elif discovery_endp.startswith('ldap:'):
        return _discover_brokers_ldap(discovery_endp, network)
    else:
        raise TypeError('Unknown discovery enpoint %s' % discovery_endp)

def _discover_brokers_ldap(discovery_endp, network):
    brokers = []
    for ep in ldapsearch('(&(objectClass=GlueService)'+\
                         '(GlueServiceType=msg.broker.stomp))',
                         ['GlueServiceEndpoint', 'GlueServiceUniqueID'],
                         discovery_endp):
        for net in ldapsearch('(&(GlueServiceDataKey=cluster)'+\
                              '(GlueChunkKey=GlueServiceUniqueID=%s))' % \
                              ep[1].get('GlueServiceUniqueID', [''])[0],
                              ['GlueServiceDataValue'],
                              discovery_endp):
            if net[1].get('GlueServiceDataValue', [''])[0] == network:
                brokers.append(ep[1].get('GlueServiceEndpoint', [''])[0])
                break
    return brokers

def _discover_brokers_file(discovery_endp, network):
    brokers = filesearch(discovery_endp, network)
    return brokers
