
from dirq import queue

Queue = queue.Queue
QueueSet = queue.QueueSet
QueueError = queue.QueueError
QueueLockError = queue.QueueLockError

__all__ = ['Queue',
           'QueueSet',
           'QueueError',
           'QueueLockError']
