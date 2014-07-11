#!/usr/bin/env python

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import sys
import time

import mesos
import mesos_pb2

TOTAL_TASKS = 2

TASK_CPUS = 1
TASK_MEM = 32

class EscalationScheduler(mesos.Scheduler):
    def __init__(self):
        self.tasksLaunched = 0
        self.tasksFinished = 0
        self.messagesSent = 0
        self.messagesReceived = 0

    def registered(self, driver, frameworkId, masterInfo):
        pass

    def resourceOffers(self, driver, offers):
        for offer in offers:
            tasks = []
            if self.tasksLaunched < TOTAL_TASKS:
                tid = self.tasksLaunched
                self.tasksLaunched += 1

                task = mesos_pb2.TaskInfo()
                task.task_id.value = str(tid)
                task.slave_id.value = offer.slave_id.value
                task.name = "task %d" % tid

                if tid == 0:
                    task.command.value = "sleep 1000"
                elif tid == 1:
                    task.command.value = "trap '' SIGTERM ; sleep 1000"

                cpus = task.resources.add()
                cpus.name = "cpus"
                cpus.type = mesos_pb2.Value.SCALAR
                cpus.scalar.value = TASK_CPUS

                mem = task.resources.add()
                mem.name = "mem"
                mem.type = mesos_pb2.Value.SCALAR
                mem.scalar.value = TASK_MEM

                tasks.append(task)
            driver.launchTasks(offer.id, tasks)

    def statusUpdate(self, driver, update):
        if update.state == mesos_pb2.TASK_RUNNING:
            driver.killTask(update.task_id)
        elif update.state == mesos_pb2.TASK_KILLED:
            if update.task_id.value == "0":
                if update.message == "Command terminated with signal Terminated: 15":
                    print "#### Passed graceful shutdown"
                else:
                    print "!!!! Failed graceful shutdown"
                    driver.stop()
            elif update.task_id.value == "1":
                if update.message == "Command terminated with signal Killed: 9":
                    print "#### Passed forced shutdown"
                else:
                    print "!!!! Failed forced shutdown"

            self.tasksFinished += 1
            if self.tasksFinished == TOTAL_TASKS:
                driver.stop()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print "Usage: %s master" % sys.argv[0]
        sys.exit(1)

    framework = mesos_pb2.FrameworkInfo()
    framework.user = ""
    framework.name = "Escalation Framework (Python)"

    driver = mesos.MesosSchedulerDriver(
        EscalationScheduler(),
        framework,
        sys.argv[1])

    status = 0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1

    # Ensure that the driver process terminates.
    driver.stop();

    sys.exit(status)
