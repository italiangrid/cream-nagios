
import re
import sys
import socket

def parse_uri3(uri):
    """Return (proto, host, port)."""
    m = re.match('([a-zA-Z0-9_]*://)?([^/:$]*):?(\d+)?/?', uri)
    try:
        proto = m.group(1).split(':')[0]
    except Exception:
        proto = ''
    return [proto, m.group(2), m.group(3)]

def parse_uri2(uri):
    """Return (host, port). Port as integer or None."""
    return parse_uri3(uri)[1:]

def dns_lookup_forward(hostname):
    """Forward DNS lookup.
    Return: list of IPs
    Raises:
    ValueError -- on empty hostname
    IOError    -- on any IP address resolution errors
    """
    if not hostname:
        raise ValueError('Empty hostname provided.')
    try:
        _, _, ips = socket.gethostbyname_ex(hostname)
    except (socket.gaierror, socket.herror), e:
        raise IOError(str(e))
    return ips

def exit_trace(msg):
    """Exit with stack trace."""
    import traceback
    exceptionType, exceptionValue, exceptionTraceback = sys.exc_info()
    sys.stderr.write('%s\n' % msg)
    traceback.print_exception(exceptionType, exceptionValue, exceptionTraceback,
                              file=sys.stdout)
    sys.stderr.write('\nReport to: https://tomtools.cern.ch/jira/browse/MIG\n')
    sys.exit(1)

def exit_err(msg, rc=1):
    sys.stderr.write('%s\n' % msg)
    sys.exit(rc)
