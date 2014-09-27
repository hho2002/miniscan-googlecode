from task import engine_plugin

class gg_plugin(engine_plugin):
    def __init__(self, name):
        engine_plugin.__init__(self, name)

    def handle_task(self, task_info):
        print task_info['work']
        pass
    
def init_plugin(name):
    return gg_plugin(name)