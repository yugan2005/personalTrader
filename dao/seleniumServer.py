import subprocess
import time
from contextlib import contextmanager


@contextmanager
def open_selenium_server():
    null_io = open('/dev/null')
    server_proc = subprocess.Popen(['selenium-server', '-port', '4444'], stdout=null_io, stderr=null_io)
    time.sleep(0.5)
    yield
    server_proc.terminate()


