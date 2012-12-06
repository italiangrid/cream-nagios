"""Simple MTA (Message Transfer Agent).
No broker connection monitoring. No SSL.

Intention: short-lived background process (on WNs) to send messages
stored in a local file-based directory queue.

Can be started before or after the directory queue was initially created and
populated with messages. Expects mig.msgdirq.message.Message messages on the
directory queue.

If a broker is not provided on command line, a discovery in BDII of all brokers
in a default (PROD) or given broker network is be performed. One broker is
chosen according to defined ranking (e.g., min message round-trip time). [The
latter is hard-coded now to 'min_roundtrip'.]

Script goes on background only after broker discovery/ranking was successful
and directory message queue was correctly set up. The process will switch to
the process group of the shell it was spawned from. The latter is for
terminal signals to the spawning shell to able to reach the process (useful
on batch systems).

Use variations of 'kill' to kill the process. It intercepts SIGTERM to
gracefully close broker connection.
"""

import os
# no os.getsid() in Python2.3
if not hasattr(os, 'getsid'):
    def _getsid(_=None):
        import commands
        rc,res = commands.getstatusoutput('ps -p %i -o sid=' % os.getpid())
        if rc != 0:
            raise SystemError("Couldn't get sid: %s" % res)
        else:
            return int(res.strip('\n').strip())
    os.getsid = _getsid

import sys
import time
import signal
import logging

from optparse import OptionParser

from mig.discovery.ldaputils import get_working_ldap
from mig.discovery.discover import discover_brokers
from mig.discovery.ranking import rank_brokers
from mig.connection.abc import ConnABC
from mig.brokerconn import BrokerConnFactory
from mig.utils import exit_trace, exit_err
from mig.log import get_logger

from msgdirq import MessageDirQueue

DEFAULT_RANK_BROKERS_MODE = 'min_roundtrip'

PIDFILE_DIR  = '/var/run'
PIDFILE_NAME = os.path.basename(sys.argv[0])
PIDFILE = '%s/%s.pid' % (PIDFILE_DIR, PIDFILE_NAME)

DIRQ_DIR = ''

BROKER_DISCOVERY = True
BROKER_NETWORK = 'PROD'
BROKER_DESTINATION = ''
BROKER_URIS = []

BDII_URIS = []

VERBOSITY = 'warn'
VERB_LVLS = [x.lower() for x in logging._levelNames.keys() if isinstance(x,str)]

def parse_args():
    global DIRQ_DIR, BROKER_NETWORK, BDII_URIS, BROKER_URIS, BROKER_DISCOVERY, \
            VERBOSITY, BROKER_DESTINATION, PIDFILE_DIR, PIDFILE

    usage="""usage: %prog -v <level> [-q|--dirq] <dirq_dir> [-b|--broker-uri] <URI>
    [-d|--destination] <dest> [-l|--ldap] <ldap-uri> [-n|--network] <network>
    -p <pidfiledir>"""
    parser = OptionParser(usage=usage)
    parser.add_option('-v', dest='verbose', default=VERBOSITY,
                      help='Verbosity level (debug|info|warn). Default: %s' %\
                      VERBOSITY)
    parser.add_option('-q','--dirq', default=DIRQ_DIR,
                      help="Directory to take messages from (required).")
    parser.add_option('-b','--broker-uri', default='',
                      help='Broker URI(s) (comma separated list).')
    parser.add_option('-d','--destination', default=BROKER_DESTINATION,
                      help='Destination on broker to send messages.')
    parser.add_option('-l','--bdii-uri', default=BDII_URIS,
                      help='BDII URI(s) (comma separated list).')
    parser.add_option('-n','--broker-network', default=BROKER_NETWORK,
                      help='Network of brokers (Default: %s).' % BROKER_NETWORK)
    parser.add_option('-p','--pidfiledir', default=PIDFILE_DIR,
                      help='Directory to store pid-file (Default: %s).' %
                      PIDFILE_DIR)
    opts,_ = parser.parse_args()

    if opts.dirq:
        DIRQ_DIR = opts.dirq
    else:
        print "[-q|--dirq] is mandatory"
        sys.exit(1)

    BROKER_NETWORK = opts.broker_network

    if opts.destination:
        BROKER_DESTINATION = opts.destination
    else:
        print "[-d|--detination] is mandatory"
        sys.exit(1)

    if opts.broker_uri:
        BROKER_URIS = opts.broker_uri.split(',')
        BROKER_DISCOVERY = False

    if BROKER_DISCOVERY:
        if opts.bdii_uri:
            BDII_URIS = opts.bdii_uri.split(',')
        else:
            BDII_URIS = [x for x in os.environ.get('LCG_GFAL_INFOSYS', '').split(',') if x]
        if not BDII_URIS:
            print "Either [-l|--bdii-uri] or $LCG_GFAL_INFOSYS should be set."
            sys.exit(1)
        # need "ldap://" as discovery schema definition
        for i,bu in enumerate(BDII_URIS[:]):
            if not bu.startswith('ldap://'):
                BDII_URIS[i] = 'ldap://'+bu

    if opts.verbose in VERB_LVLS:
        VERBOSITY = opts.verbose
    else:
        print "-v can be (debug, info, warn)"
        sys.exit(1)

    if opts.pidfiledir:
        PIDFILE_DIR = opts.pidfiledir
    PIDFILE = '%s/%s.pid' % (PIDFILE_DIR, PIDFILE_NAME)

def get_broker():
    global BROKER_URIS
    if BROKER_DISCOVERY:
        try:
            ldap = get_working_ldap(BDII_URIS)
            BROKER_URIS = discover_brokers(ldap, network=BROKER_NETWORK)
        except Exception, e:
            exit_trace("Broker discovery failed: %s" % str(e))
    try:
        if len(BROKER_URIS) > 1:
            return rank_brokers(BROKER_URIS, mode=DEFAULT_RANK_BROKERS_MODE)
        else:
            return rank_brokers(BROKER_URIS, mode='first_connect')
    except Exception, e:
        exit_trace("Ranking brokers failed: %s" % str(e))

def demonize(keep_pgid=False):
    try:
        pid = os.fork()
    except OSError, e:
        print "ERROR: Cound't fork: %s" % str(e)
        sys.exit(1)
    """Go on background and re-set the process' group id to the session id
    to stay reachable by signals to our old controlling terminal."""
    if pid == 0:
        if keep_pgid:
            os.setpgid(0, os.getsid(0))
    else:
        os._exit(0)
    save_pid()

def save_pid():
    open(PIDFILE, 'w').write(str(os.getpid()))

BC = object
def do_cleanup():
    if isinstance(BC, ConnABC) and BC.is_connected():
        try:
            BC.disconnect()
        except: pass
    try:
        os.unlink(PIDFILE)
    except: pass

def sigterm_handler(signum, frame):
    do_cleanup()
    sys.exit(0)

def main():
    global BC
    signal.signal(signal.SIGTERM, sigterm_handler)

    parse_args()

    broker_uri = get_broker()
    if not broker_uri:
        exit_err("Found no working broker we could connect to.")
    print "Message Broker URI:", broker_uri
    print "Message Broker destination:", BROKER_DESTINATION

    ssl_opts = {}
    try:
        BC = BrokerConnFactory(broker_uri, verbosity=VERBOSITY, **ssl_opts)
    except Exception, e:
        exit_trace("Couldn't connect to broker %s: %s" % (broker_uri, str(e)))
    else:
        if not BC.is_connected():
            exit_err("Couldn't connect to broker %s" % broker_uri)
    try:
        mq = MessageDirQueue(DIRQ_DIR)
    except Exception, e:
        exit_trace("Couldn't initialise message dir queue: %s" % str(e))

    demonize(keep_pgid=True)

    headers = {'destination':BROKER_DESTINATION, 'persistent':'true'}
    while True:
        for e in mq:
            m = mq.dequeue_message(e)
            m.header_update(headers)
            BC.send(m)
        time.sleep(1)
        get_logger().info("slept in message dispatch loop.")
    do_cleanup()
