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

------------------------------------------------------------------------------------------------
Nagios plugin which submits a job to CREAM CE, checks its terminal status and finally purges it. 
------------------------------------------------------------------------------------------------
"""
__author__ = "Lisa Zangrando lisa.zangrando@pd.infn.it"
__date__ = "27.09.2013"
__version__ = "0.1.0"

import time
import dircache
import shutil
from cream import Client

def main():
    client = Client("cream-jobSubmit", "1.0")
    client.createParser()
    client.readOptions()

    try:
        client.checkProxy()

        jobId = client.jobSubmit()

        client.debug("job id: " + jobId)
    except Exception as ex:
        client.nagiosExit(client.CRITICAL, ex)

    terminalStates = ['DONE-OK', 'DONE-FAILED', 'ABORTED', 'CANCELLED']
    lastStatus = ""
    exitCode = None

    while not lastStatus in terminalStates:
        time.sleep(10)
        try:
            lastStatus, exitCode = client.jobStatus(jobId)

            client.debug("job status: " + lastStatus)
        except Exception as ex:
            client.nagiosExit(client.CRITICAL, ex)


    outputSandbox = None
    lastLine = None

    #if lastStatus == "DONE-OK":
    try:
        osbdir = client.getOutputSandbox(jobId)

        client.debug("output sandbox dir: " + osbdir)

        dir = dircache.listdir(osbdir)

        outputSandbox = ""
        lastLine = ""

        for file in dir:
            with open(osbdir + "/" + file) as infile:
                outputSandbox += "\n-------------------------\n" + file + "\n-------------------------\n"

                for line in infile:
                     outputSandbox += line
                     lastLine = line

        shutil.rmtree(osbdir)
    except Exception as ex:
        client.nagiosExit(client.CRITICAL, ex)

    try:       
        client.jobPurge(jobId)
    except Exception as ex:
        client.debug("cannot purge the job" + ex)

    if lastStatus == terminalStates[0] and exitCode == "0":
        client.nagiosExit(client.OK, lastStatus + ": " + lastLine)
    else:
        client.nagiosExit(client.CRITICAL, "Job terminated with status=" + lastStatus + " and exitCode=" + exitCode + outputSandbox)



if __name__ == '__main__':
    main()

