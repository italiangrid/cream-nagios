#!/usr/bin/env python
"""
Copyright (c) Members of the EGEE Collaboration. 2006-2010.
See http://www.eu-egee.org/partners/ for details on the copyright holders.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

-------------------------------------------------------------
Nagios plugin which submits a job to CREAM CE and purges it.
-------------------------------------------------------------
"""
__author__ = "Lisa Zangrando lisa.zangrando@pd.infn.it"
__date__ = "27.09.2013"
__version__ = "0.1.0"

import time
from cream_cli import Client

def main():
    client = Client("cream_jobPurge", "1.0")
    client.createParser()
    client.readOptions()

    try:
        client.checkProxy()

        jobId = client.jobSubmit()

        client.debug("job id: " + jobId)
    except Exception as ex:
        client.nagiosExit(client.CRITICAL, ex)

    terminalStates = ['DONE-OK', 'DONE-FAILED', 'ABORTED', 'CANCELLED']

    lastStatus=""

    while not lastStatus in terminalStates:
        time.sleep(10)
        try:
            lastStatus, exitCode = client.jobStatus(jobId)

            client.debug("job status: " + lastStatus)
        except Exception as ex:
            client.nagiosExit(client.CRITICAL, ex)

    try:       
        client.jobPurge(jobId)
    except Exception as ex:
        client.nagiosExit(client.CRITICAL, ex)

    while (1):
        time.sleep(10)
        try:
            lastStatus, exitCode = client.jobStatus(jobId)

            client.debug("job status: " + lastStatus)
        except Exception as ex:
            client.nagiosExit(client.OK, "OK: job purged")

if __name__ == '__main__':
    main()

