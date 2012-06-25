from engine import *
import socket, struct

class test_plugin(engine_plugin):
    def __init__(self, name):
        engine_plugin.__init__(self, name)
    
    def handle_task(self, task):
        ip =  socket.inet_ntoa(struct.pack("L", socket.htonl(task)))
        for port in self.get_cfg_vaule("ports").split(" "):
            try:
                sock = socket.create_connection((ip, int(port)), 1)
                if sock:
                    self.log("handle_task: %s %d open!" % (ip, int(port)))
                    sock.close()
            except:
                pass
            
def init_plugin(name):
    return test_plugin(name)
