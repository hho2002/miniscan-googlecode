from task import *
import socket, struct

class port_plugin(engine_plugin):
    def __init__(self, name):
        engine_plugin.__init__(self, name)
    
    def handle_task(self, task):
        name, task = task
        ip =  socket.inet_ntoa(struct.pack("L", socket.htonl(task)))
        self.log("%s handle_task: %s" % (name, ip))
        for port in self.get_cfg_vaule("ports").split(" "):
            try:
                sock = socket.create_connection((ip, int(port)), 1)
                if sock:
                    self.log("%s handle_task: %s %d open!" % (name, ip, int(port)))
                    sock.close()
            except:
                pass
            
def init_plugin(name):
    return port_plugin(name)
