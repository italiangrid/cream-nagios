
"""Uses Python ldap API by default. If not available,
falls back to ldapsearch CLI.
"""

import os
import commands

from mig import utils

try:
    import ldap
    LDAP_LIB = True
except ImportError:
    LDAP_LIB = False

__all__ = ['ldapsearch',
           'get_working_ldap',
           'ldapbind',
           'NoWorkingLDAPError',
           'LDAPSearchError']

LDAP_TIMEOUT_NETWORK  = 20
LDAP_TIMELIMIT_SEARCH = 20

class NoWorkingLDAPError(Exception):
    """No working LDAP found."""
class LDAPSearchError(Exception):
    """Error searching in LDAP."""

def ldapsearch(filter, attrlist, uri, base='o=grid',
               searchtimeout=LDAP_TIMELIMIT_SEARCH,
               net_timeout=LDAP_TIMEOUT_NETWORK):
    """Query LDAP.

    filter - non-empty string
    attrlist - list of attributes
    uri - ldap://<hostname|ip>:port

    Return:
    [entries] - entries : list of tuples as query results
                ('<LDAPnameSpace>', {'<attribute>': ['<value>',..],..})
    Raise:
    ValueError - empty filter
    TypeError  - attrlist is not a list object
    LDAPSearchError - on errors in LDAP search backends
    """
    if not filter:
        raise ValueError('filter must not be empty.')
    if not isinstance(attrlist, list):
        raise TypeError('attrlist should be list.')

    try:
        if LDAP_LIB:
            return _ldapsearch_API(filter, attrlist, uri, base,
                                    searchtimeout, net_timeout)
        else:
            return _ldapsearch_CLI(filter, attrlist, uri, base,
                                    searchtimeout, net_timeout)
    except Exception, e:
        raise LDAPSearchError(str(e))

def _ldapsearch_API(filter, attrlist, uri, base, searchtimeout, net_timetout):
    """Query LDAP using API.
    uri - ldap://<hostname|ip>:port
    """

    l = ldap.initialize(uri)
    l.protocol_version = ldap.VERSION3
    l.network_timeout = net_timetout
    entries = l.search_st(base, ldap.SCOPE_SUBTREE,
                              filter, attrlist,
                              0, searchtimeout)
    return entries

def _ldapsearch_CLI(filter, attrlist, uri, base, searchtimeout, net_timetout):
    """Query LDAP using CLI.
    uri - ldap://<hostname|ip>:port
    """
# TODO: -o nettimeout seem doesn't properly work on all WNs
#    cmd = "ldapsearch -l %i -x -LLL -H %s -o nettimeout=%i -b %s '%s' %s" % \
#            (searchtimeout, uri, net_timetout, base, filter,
#             ' '.join([x for x in attrlist]))
    cmd = "ldapsearch -l %i -x -LLL -H %s -b %s '%s' %s" % \
            (searchtimeout, uri, base, filter,
             ' '.join([x for x in attrlist]))
    res = ''
    try:
        rc,res = commands.getstatusoutput(cmd)
    except StandardError,e:
        raise StandardError('%s\n%s %s' % (cmd, str(e).strip(), uri))
    else:
        rc = os.WEXITSTATUS(rc)
        if rc != 0:
            raise LDAPSearchError('%s\n%s %s %i' % (cmd, res, uri, rc))
    if res:
        # remove line foldings made by ldapsearch
        res = res.replace('\n ','').strip()
        entries = []
        res = res.split('dn: ')
        # loop through values in "dn:"
        for dn in res:
            if dn:
                dl = dn.splitlines()
                # remove empty lines
                for i,v in enumerate(dl):
                    if not v:
                        del dl[i]
                # make dict key/value pairs out
                # of Glue "Attribute: Value" pairs
                d = {}
                for x in dl[1:]:
                    t = x.split(':', 1)
                    t[0] = t[0].strip()
                    t[1] = t[1].strip()
                    if d.has_key(t[0]):
                        d[t[0]].append(t[1])
                    else:
                        d[t[0]] = [t[1]]
                entries.append((dl[0], d))
        return entries
    else:
        return []

def get_working_ldap(ldaps):
    """Return first working LDAP endpoint (with IP).
    ldaps - list of LDAP endpoints.
    """
    errors = {}
    for ldap in ldaps:
        proto, host, port = utils.parse_uri3(ldap)
        ips = utils.dns_lookup_forward(host)
        for ip in ips:
            _ldap = '%s%s%s' % (proto and proto+'://' or '',
                                ip, port and ':'+port or '')
            rc, error = ldapbind(_ldap)
            if rc:
                return _ldap
            else:
                errors[_ldap] = error
    raise NoWorkingLDAPError("Couldn't get working LDAP from %s:\n%s" % (ldaps,
                         '\n'.join(['%s: %s'%(k,v) for k,v in errors.items()])))

def ldapbind(url):
    """
    Return:
    (1, '') - on success
    (0, 'error msg') - on failure
    """
    if LDAP_LIB:
        return _ldapbind_API(url)
    else:
        return _ldapbind_CLI(url)

def _ldapbind_API(url, net_timeout=LDAP_TIMEOUT_NETWORK):
    try:
        l = ldap.initialize(_to_full_ldap_uri(url))
        l.network_timeout = net_timeout
        l.simple_bind()
        l.unbind()
    except ldap.LDAPError, e:
        return 0, 'LDAPError: %s' % e[0]['desc']
    return 1, ''

def _ldapbind_CLI(uri, net_timetout=LDAP_TIMEOUT_NETWORK):
# TODO: move back to version with network timeout when WNs are ready.
# cmd = 'ldapsearch -xLLL -o nettimeout=%i -H %s' % (net_timetout,
#                                                   _to_full_bdii_uri(uri))
    cmd = 'ldapsearch -xLLL -H %s' % (_to_full_ldap_uri(uri))
    rc, o = commands.getstatusoutput(cmd)
    rc = os.WEXITSTATUS(rc)
    if rc not in (0, 32): # No such object (32)
        return 0, '%s , %i' % (o, rc)
    return 1, ''

def _to_full_ldap_uri(uri, port='2170'):
    php = utils.parse_uri3(uri)
    if not php[0]:
        php[0] = 'ldap'
    if not php[2]:
        php[2] = port
    return '%s://%s:%s' % (php[0], php[1], str(php[2]))
