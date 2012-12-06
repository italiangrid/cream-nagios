# encoding: utf-8
"""
MessageDirQueue class
"""
from dirq import Queue as DirQueue
from msgdirq.message import Message

class MessageDirQueue(DirQueue):
    """Queue of messages on a file system.
    """
    schema = {'header'     : 'table',
              'text_body'  : 'string?',
              'binary_body': 'binary?'}

    def __init__(self, path, umask=None, maxelts=16000):
        DirQueue.__init__(self, path, umask=None, maxelts=maxelts,
                       schema=self.schema)

    def add_message(self, msg):
        """
        """
        return self.add({'header': msg.header,
                         msg.is_text and 'text_body' or 'binary_body':msg.body})

    def get_message(self, ename):
        """
        """
        el = self.get(ename)
        if el.has_key('text_body') and el.has_key('binary_body'):
            raise TypeError('text_body and binary_body are not allowed in element.')
        try:
            body = el['text_body']
        except KeyError:
            try:
                body = el['binary_body']
            except KeyError:
                body = ''
        return Message(body, el['header'])

    def dequeue_message(self, ename):
        """
        """
        self.lock(ename)
        m = self.get_message(ename)
        self.remove(ename)
        return m

if __name__ == "__main__":
    # TODO: non-ascii chars in unicode sting don't work!!!!
    #msg = Message(u'Hello Worldé', {u'aé':'b','c':'dù'})
    msg = Message(u'Hello World', {'a':'b','c':'d'})
    mq = MessageDirQueue('/tmp/mq.test')
    mq.add_message(msg)
    mq.add_message(msg)
    mq.add_message(msg)

    print "Browse messages:"
    for e in mq:
        mq.lock(e)
        m = mq.get_message(e)
        mq.unlock(e)
        print m.header
        print m.body
        print '-'*3

#    print "Dequeue messages:"
#    for e in mq:
#        m = mq.dequeue_message(e)
#        print m.header
#        print m.body
#        print '-'*3

    print msg == m
