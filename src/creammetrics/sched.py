# Copyright (c) Members of the EGEE Collaboration. 2004. 
# See http://www.eu-egee.org/partners/ for details on the copyright
# holders.  
#
# Licensed under the Apache License, Version 2.0 (the "License"); 
# you may not use this file except in compliance with the License. 
# You may obtain a copy of the License at 
#
#     http://www.apache.org/licenses/LICENSE-2.0 
#
# Unless required by applicable law or agreed to in writing, software 
# distributed under the License is distributed on an "AS IS" BASIS, 
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
# See the License for the specific language governing permissions and 
# limitations under the License.

#
# Authors: Konstantin Skaburskas <konstantin.skaburskas@cern.ch>, CERN
#

"""
threaded scheduler.

Threaded scheduler.

Konstantin Skaburskas <konstantin.skaburskas@cern.ch>, CERN
SAM (Service Availability Monitoring)
"""

import time

import thread
mutex = thread.allocate_lock()
threads_pool = {}

def de_queue(h):
    mutex.acquire()
    del threads_pool[h]
    mutex.release()

class SchedulerThread:
    def __init__(self, max_threads=10, default_timeout=300):
        self.max_threads = max_threads
        self.default_timeout = default_timeout

    def check_threads(self):
        global threads_pool
        t = time.time()

        for h, v in threads_pool.items():
            if t > v['t']:
                try:
                    # kill processes spawned by the thread
                    # we need a global object holding PIDs
                    print "Thread timeout. Killing processes spawned by : ",h
#                    os.kill(-pid, signal.SIGTERM)
#                    os.waitpid(pid, 0)
                except StandardError:
                    pass
                del threads_pool[h]
#            else:
#                if os.waitpid(pid,os.WNOHANG)[0]:
#                    del self.threads_pool[h]

    def wait(self):
        global threads_pool
        self.check_threads()
        while threads_pool:
            time.sleep(1)
            self.check_threads()

    def run(self, func, jd, host, timeout=None):
        global threads_pool
        self.check_threads()
        while len(threads_pool) >= self.max_threads:
            time.sleep(1)
            self.check_threads()

        tm = timeout or self.default_timeout
        threads_pool[host] = {'jd' : jd,
                              't'  : time.time()+tm}
        thread.start_new_thread(func, (jd,))



