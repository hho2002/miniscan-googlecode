from task import *
import socket, struct

class web_plugin(engine_plugin):
    def __init__(self, name):
        engine_plugin.__init__(self, name)
    
    def handle_task(self, task):
        name , (host, url) = task
        self.log("%s handle_task: %s %s" % (name, host, url))
        
def init_plugin(name):
    return web_plugin(name)