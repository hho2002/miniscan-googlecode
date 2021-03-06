from task import *
import socket, struct, time

class port_plugin(engine_plugin):
    
    def __init__(self, name):
        engine_plugin.__init__(self, name)
        #self.max_process = 20
        
    def handle_task(self, task_info):
        ip =  socket.inet_ntoa(struct.pack("L", socket.htonl(task_info['work'])))
        #self.log(task_info, "handle_task: %s process:%d" %  (ip, task_info['process']))
        print "\r>>%s\t" % ip,
        for port in self.get_cfg_vaule(task_info, "ports").split(" "):
            try:
                sock = socket.create_connection((ip, int(port)), 1)
                if sock:
                    self.log(task_info, "%s\t%d\topen" % (ip, int(port)))
                    sock.close()
            except:
                pass
            
def init_plugin(name):
    return port_plugin(name)
