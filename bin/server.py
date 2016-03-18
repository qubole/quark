#!/usr/bin/env python
############################################################################
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
############################################################################

#
# Script to handle launching the query server process.
#
# usage: server.py start|stop|makeWinServiceDesc json-filename [jars]
#

import datetime
import getpass
import os
import os.path
import signal
import subprocess
import sys
import fnmatch
try:
    import daemon
    from daemon import pidfile
    daemon_supported = True
except ImportError:
    daemon_supported = False

def getPath():
    QUARK_SERVER_JAR_PATTERN = "quark-server-*.jar"

    current_dir = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(current_dir, "..", "server", "target", "*")

    if ((path is not None) and (len(path) > 0) and (path[-1] == '*')) :
        path = path[:-1]

    for root, dirs, files in os.walk(path):
        for name in files:
            if fnmatch.fnmatch(name, QUARK_SERVER_JAR_PATTERN):
                return os.path.join(root, name)


quark_server_jar = getPath()
command = None
args = sys.argv


if len(args) > 1:
    if args[1] == 'start':
        command = 'start'
    elif args[1] == 'stop':
        command = 'stop'
    elif args[1] == 'makeWinServiceDesc':
        command = 'makeWinServiceDesc'
    else:
        print >> sys.stderr, "Command '" + args[1] + "' not supported. Available commands are (start, stop, makeWinServiceDesc)."
        sys.exit(-1)
else:
    print >> sys.stderr, "No command given to the server. Available commands are (start, stop, makeWinServiceDesc)"
    sys.exit(-1)

args = args[2:]

filename = ''
if len(args) > 0:
    filename = args[0]
    args = args[1:]
# Create jar string
jar_string = quark_server_jar
if len(args) > 0:
    for jar in args:
        jar_string = jar_string + ':' + jar

if os.name == 'nt':
    args = subprocess.list2cmdline(args)
else:
    import pipes    # pipes module isn't available on Windows
    args = " ".join([pipes.quote(v) for v in args])
java_home = os.getenv('JAVA_HOME')

quark_log_dir = os.path.dirname(os.path.abspath(__file__))

out_file_path = os.path.join(quark_log_dir, "quark-server.log")
pid_file_path = os.path.join(quark_log_dir, "quark-server.pid")

if java_home:
    java = os.path.join(java_home, 'bin', 'java')
else:
    java = 'java'

# The command is run through subprocess so environment variables are automatically inherited
java_cmd = '%(java)s -Dproc_quarkserver -cp ' + jar_string + \
        ' com.qubole.quark.server.Main ' + filename

if command == 'makeWinServiceDesc':
    cmd = java_cmd % {'java': java}
    slices = cmd.split(' ')

    print "<service>"
    print "  <name>Quark Server</name>"
    print "  <description>This service runs the Quark Server.</description>"
    print "  <executable>%s</executable>" % slices[0]
    print "  <arguments>%s</arguments>" % ' '.join(slices[1:])
    print "</service>"
    sys.exit()


if command == 'start':
    if not daemon_supported:
        print >> sys.stderr, "daemon mode not supported on this platform"
        sys.exit(-1)

    if filename == '':
        print >> sys.stderr, "Need config file as input"
        sys.exit(-1)

    # run in the background
    d = os.path.dirname(out_file_path)
    if not os.path.exists(d):
        os.makedirs(d)

    with open(out_file_path, 'a+') as out:
        context = daemon.DaemonContext(
                pidfile = pidfile.PIDLockFile(pid_file_path),
                stdout = out,
                stderr = out,
        )
        with context:
            # this block is the main() for the forked daemon process
            child = None
            cmd = java_cmd % {'java': java}
            print >> sys.stderr, cmd

            # notify the child when we're killed
            def handler(signum, frame):
                if child:
                    child.send_signal(signum)
                sys.exit(0)
            signal.signal(signal.SIGTERM, handler)

            child = subprocess.Popen(cmd.split())
            sys.exit(child.wait())


elif command == 'stop':
    if not daemon_supported:
        print >> sys.stderr, "daemon mode not supported on this platform"
        sys.exit(-1)

    if not os.path.exists(pid_file_path ):
        print >> sys.stderr, "no Query Server to stop because PID file not found, %s" % pid_file_path
        sys.exit(0)

    if not os.path.isfile(pid_file_path):
        print >> sys.stderr, "PID path exists but is not a file! %s" % pid_file_path
        sys.exit(1)

    pid = None
    with open(pid_file_path, 'r') as p:
        pid = int(p.read())
    if not pid:
        sys.exit("cannot read PID file, %s" % pid_file_path)

    print "stopping Query Server pid %s" % pid
    with open(out_file_path, 'a+') as out:
        print >> out, "%s terminating Server" % datetime.datetime.now()
    os.kill(pid, signal.SIGTERM)

else:
    cmd = java_cmd % {'java': java}
    child = subprocess.Popen(cmd.split())
    sys.exit(child.wait())
