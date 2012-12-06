"""ABCs.
"""

class ConnABC(object):
    ''
    def send(self, m):
        raise NotImplementedError("send() not implemented.")
    def stop(self):
        raise NotImplementedError("stop() not implemented.")
    def disconnect(self):
        raise NotImplementedError("disconnect() not implemented.")

class MessageABC(object):
    ''
