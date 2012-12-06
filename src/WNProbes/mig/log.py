
import sys
import logging

# configure logger manually to be Python 2.3 compliant
log = logging.getLogger('ConnStomp')
hdlr = logging.StreamHandler(sys.stdout)
hdlr.setFormatter(
        logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s'))
log.addHandler(hdlr)
log.setLevel(logging.WARNING)

def get_logger():
    return log
