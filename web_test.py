from task import *
import socket, struct

class web_plugin(engine_plugin):
    def __init__(self, name):
        engine_plugin.__init__(self, name)
    
    def handle_task(self, task):
        host, url = task
        self.log("handle_task: %s %s" % (host, url))
        
def init_plugin(name):
    return web_plugin(name)