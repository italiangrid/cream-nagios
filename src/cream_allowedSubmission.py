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

-------------------------------------------------------
Nagios plugin which checks if the submission is allowed 
-------------------------------------------------------
"""
__author__ = "Lisa Zangrando lisa.zangrando@pd.infn.it"
__date__ = "12.12.2019"
__version__ = "0.1.1"

from cream_cli.cream import Client

def main():
    client = Client("cream_allowedSubmission", "1.1")
    client.createParser("FALSE")
    client.readOptions()

    try:
        client.checkProxy()

        data = client.allowedSubmission()
        client.nagiosExit(client.OK, "CREAM allowedSubmission OK: the job submission is %s" % data)
    except Exception as ex:
        client.nagiosExit(client.CRITICAL, "CREAM allowedSubmission ERROR: %s" % ex)


if __name__ == '__main__':
    main()

