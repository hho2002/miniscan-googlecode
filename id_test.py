from task import engine_plugin
import hashlib

pwd_list = [
'1',
'111',
'222',
'admin',
'xxx',
'12345678',
'123456',
'test',
'0000',
'pwd',
'pass',
'password',
'!@#$%^',
'hello',
'test123',
'admin123',
'admin123456',
'qwert',
'qwerty',
'AAA']

class port_plugin(engine_plugin):
    def __init__(self, name):
        engine_plugin.__init__(self, name)
        #self.max_process = 20
        
    def handle_task(self, task_info):
        self.log(task_info, "handle_task: ID %d process:%d" % \
                 (task_info['work'], task_info['process']))
        index = task_info['work']
        for md5 in self.get_cfg_vaule(task_info, "md5").split(" "):
            pwd = pwd_list[index]
            if md5 == hashlib.md5(pwd).hexdigest():
                self.log(task_info, "%s md5 %s ok!" % (pwd, md5))
        
def init_plugin(name):
    return port_plugin(name)
