#!/usr/bin/env python3
import subprocess
import time
import fcntl
import os
# def nonBlockRead(output):
#     fd = output.fileno()
#     fl = fcntl.fcntl(fd, fcntl.F_GETFL)
#     fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NONBLOCK)
#     try:
#         text = output.read();
#         if(text is None):
#             return ''
#         return str(text,'utf-8')
#     except:
#         return ''
def nonBlockRead(output):
    fd = output.fileno()
    fl = fcntl.fcntl(fd, fcntl.F_GETFL)
    fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NONBLOCK)
    try:
        text = output.read();
        if(text is None):
            return ''
        return str(text,'utf-8')
    except:
        return ''
def execute_cmd(cmdline, timeout):
    """
    Execute cmdline, limit execution time to 'timeout' seconds.
    Uses the subprocess module and subprocess.PIPE.
    Raises TimeoutInterrupt
    """
    cmdline="/opt/client/HDFS/hadoop/bin/yarn node -list 2> /dev/null | grep RUNNING"
    p = subprocess.Popen(
        cmdline,
        bufsize=0,  # default value of 0 (unbuffered) is best
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    t_begin = time.time()  # Monitor execution time
    seconds_passed = 0
    stdout = ''
    stderr = ''
    while p.poll() is None and (
            seconds_passed < timeout or timeout == 0):  # Monitor process
        time.sleep(0.1)  # Wait a little
        seconds_passed = time.time() - t_begin
        stdout += nonBlockRead(p.stdout)
        stderr += nonBlockRead(p.stderr)
        print(stdout)
        print(stderr)
    if seconds_passed >= timeout and timeout > 0:
        try:
            p.stdout.close()  # If they are not closed the fds will hang around until
            p.stderr.close()  # os.fdlimit is exceeded and cause a nasty exception
            p.terminate()     # Important to close the fds prior to terminating the process!
            # NOTE: Are there any other "non-freed" resources?
        except:
            pass
        return ('Timeout', stdout, stderr)
    return (p.returncode, stdout, stderr)
execute_cmd("",5)
