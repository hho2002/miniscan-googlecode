from task import *
import socket, struct, select

class banner_plugin(engine_plugin):
    
    def __init__(self, name):
        engine_plugin.__init__(self, name)
        
    def handle_task(self, task_info):
        ip =  socket.inet_ntoa(struct.pack("L", socket.htonl(task_info['work'])))
        print "\r>>%s\t" % ip,
        
        try:
            time_out = int(self.get_cfg_vaule(task_info, "timeout"))
        except:
            time_out = 3
            
        for port in self.get_cfg_vaule(task_info, "ports").split(" "):
            try:
                sock = socket.create_connection((ip, int(port)), time_out)
                if sock:
                    #sock.setblocking(0)
                    ready = select.select([sock], [], [], time_out)
                    if ready[0]:
                        banner = sock.recv(1024)                        
                        self.log(task_info, "%s\t%d\t%s" % (ip, int(port), banner))
                    sock.close()
            except:
                pass
            
def init_plugin(name):
    return banner_plugin(name)
