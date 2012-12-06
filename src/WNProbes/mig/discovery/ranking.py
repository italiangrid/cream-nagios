
import os
import sys
import time
import random

# # REMOVE THIS FROM HERE!!!!!!!
import stomp
# REMOVE THIS FROM HERE!!!!!!!

from mig import utils

DEFAULT_BROKER_RANKING_MODE = 'best_roundtrip'
BROKER_RANKING_MODES = ['first_connect',
                        'rand_first_connect',
                        'min_connect',
                        'min_roundtrip']
BROKER_RANKING_MAX_MSG_WAIT = 2. # 2 sec

def rank_brokers(brokers, mode=DEFAULT_BROKER_RANKING_MODE):
    """
    """
    if mode not in BROKER_RANKING_MODES:
        mode = DEFAULT_BROKER_RANKING_MODE
    ranker = getattr(sys.modules[__name__], '_get_broker_%s' % mode)
    return ranker(brokers)

def _get_broker_first_connect(brokers):
    for uri in brokers:
        host, port = utils.parse_uri2(uri)
        conn = stomp.Connection([(host, int(port))])
        conn.start()
        conn.connect()
        if conn.is_connected():
            conn.stop()
            return uri
        conn.stop()

def _get_broker_rand_first_connect(brokers):
    random.shuffle(brokers)
    return _get_broker_first_connect(brokers)

def _get_broker_min_connect(brokers):
    conntimes_uris = {}
    for uri in brokers:
        host, port = utils.parse_uri2(uri)
        conn = stomp.Connection([(host, int(port))])
        t1 = time.time()
        conn.start()
        conn.connect()
        ctime = time.time() - t1
        if conn.is_connected():
            conntimes_uris[ctime] = uri
        try:
            conn.stop()
        except:
            pass
    if conntimes_uris:
        ctimes = conntimes_uris.keys()
        ctimes.sort()
        return conntimes_uris[ctimes[0]]

def _get_broker_min_roundtrip(brokers):
    destination = '/temp-queue/roundtriptest'
    id = str(time.time()+os.getpid())
    import string
    msg_body = string.ascii_letters
    class _listener(object):
        message = False
        def on_message(self, header, body):
            if body == msg_body:
                self.message = True
    listener = _listener()
    triptimes_uris = {}
    for uri in brokers:
        host, port = utils.parse_uri2(uri)
        conn = stomp.Connection([(host, int(port))])
        conn.set_listener('listener', listener)
        t1 = time.time()
        conn.start()
        conn.connect()
        if conn.is_connected():
            conn.subscribe({'destination':destination, 'id':id})
            conn.send(msg_body, {'destination':destination})
            t = 0
            while not listener.message and t < BROKER_RANKING_MAX_MSG_WAIT:
                time.sleep(0.1)
                t += 0.1
            if listener.message:
                conn.unsubscribe({'id':id})
                ttime = time.time() - t1
                triptimes_uris[ttime] = uri
            else:
                conn.unsubscribe({'id':id})
        listener.message = False
        try:
            conn.stop()
        except:
            pass
        del conn
    if triptimes_uris:
        ttimes = triptimes_uris.keys()
        ttimes.sort()
        return triptimes_uris[ttimes[0]]
