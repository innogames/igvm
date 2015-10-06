import time
import socket

from fabric.api import puts

def ping_port(ip, port=22, timeout=1):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(timeout)
    try:
        s.connect((ip, port))
    except (socket.timeout, socket.error):
        return False
    else:
        return True
    finally:
        s.close()

def wait_until(ip, port=22, timeout=60, waitmsg=None):
    if waitmsg:
        puts(waitmsg)

    for sec in xrange(timeout):
        if ping_port(ip, port):
            return True

        if waitmsg:
            puts('Remaining: {0} secs'.format(timeout - sec))
        time.sleep(1)

    return False
