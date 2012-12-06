# encoding: utf-8
"""
Message class
"""

# TODO: Consider removing. Only helper functions are used:
#       queue._dict2string() and queue._string2dict()
from dirq import queue

class Message(object):
    """
    """
    def __init__(self, body, header={}):
        self.__set_body(body)
        self.__set_header(header)

    def __set_body(self, body):
        if not isinstance(body, basestring):
            raise TypeError('body should be byte string or unicode.')
        self.__body = body
        if isinstance(body, unicode):
            self.__set_is_text(True)
        else:
            self.__set_is_text(False)
    def __get_body(self):
        return self.__body
    body = property(__get_body, __set_body, None,
                    "get/set body attribute as byte string or unicode.")

    def __set_is_text(self, v):
        self.__is_text = v
    def __get_is_text(self):
        return self.__is_text
    is_text = property(__get_is_text, __set_is_text, None,
                       """
                       get/set is_text attribute. Boolean indicating if the
                       message body is a text string (unicode) or not.""")

    def __set_header(self, header):
        if not isinstance(header, dict):
            raise TypeError('header should be dictionary.')
        for k,v in header.items():
            if not isinstance(k, basestring) or not isinstance(v, basestring):
                raise TypeError("keys/values should be byte strings or unicode.")
        self.__header = header
    def __get_header(self):
        return self.__header
    header = property(__get_header, __set_header, None,
                      """
                      get/set header attributes as a dictionary.
                      Byte or unicode strings are only allowed.""")

    def header_field(self, *args):
        """get/set header fields.
        Set:
        - with two arguments updates header with the key/value pair
        Get:
        - with one argument returns value corresponding to the key or
        KeyError is raised
        """
        if len(args) == 1:
            return self.__header[args[0]]
        elif len(args) == 2:
            if not isinstance(args[0], basestring):
                raise TypeError("key should be basestring.")
            if not isinstance(args[1], basestring):
                raise TypeError("value should be basestring.")
            self.__header[args[0]] = args[1]

    def header_update(self, headers):
        """Update header.
        """
        if isinstance(headers, dict):
            self.__header.update(headers)
        else:
            raise TypeError("Expected dict, %s given." % type(headers))

    def serialize(self):
        try:
            bs = queue._dict2string(self.header).strip('\n') + '\n\n'
        except queue.QueueError, e:
            raise TypeError(str(e))
        if self.is_text:
            bs += self.body.encode('utf-8')
        else:
            bs += self.body
        return bs

    # class method Python2.3 way; can't use @classmethod decorator
    def deserialize(self, string):
        (header, body) = string.split('\n\n', 1)
        try:
            return Message(body, queue._string2dict(header))
        except queue.QueueError, e:
            raise TypeError(str(e))
    deserialize = classmethod(deserialize)

    def __str__(self):
        return self.serialize()
    def __eq__(self, other):
        if self.body == other.body and self.header == other.header:
            return True
        else:
            return False

if __name__ == "__main__":
    msg = Message("hello world!", {'aé':'b','c':'dù'})
#    print msg.is_text
#    print '-'
#    print msg.header
#    print '-'
#    print msg.header_field('123', '1234')
#    print '-'
#    print msg.header
#    print '-'
#    print msg.header_field('123')
#    print '-'

    msg.body = 'Hello World!é'
    print msg.body
    print msg.is_text

    print '----'
    print msg.serialize()
    print '----'
    m = Message.deserialize(msg.serialize())
    print m
    print m.header
    print m.body
    print m == msg
