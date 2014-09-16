from task import *
import urllib2, httplib

class http_prob_plugin(engine_plugin):
    def __init__(self, name):
        engine_plugin.__init__(self, name)
        
    def http_banner_prob(self, host, port, is_https = False):
        try:
            if is_https:
                h = httplib.HTTPS(host, port)
            else:
                h = httplib.HTTP(host, port)

            h.putrequest('GET', '/')
            h.putheader('Host', host+':'+port)
            h.putheader('User-agent', "Mozilla/5.0 (Windows NT 5.1; rv:26.0) Gecko/20100101 Firefox/26.0")
            h.putheader('Accept', 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8')
            h.putheader('Accept-Language','en-us')
            h.putheader('Accept-Encoding','gzip, deflate')
            
            h.endheaders()

            statuscode, statusmessage, headers = h.getreply()

            #if 'HFS' in headers['Server']:
            if headers['Server']:
                #if statuscode==200:
                return headers

        except :
            pass
        return None
    
    def handle_task(self, task_info):
        ip =  socket.inet_ntoa(struct.pack("I", socket.htonl(task_info['work'])))
        print "\r>>: %s" % ip,

        try:
            sock = socket.create_connection((ip, 80), 4)
            sock.close()
            
            ret = self.http_banner_prob(ip, '80');
            if ret:
                self.log(task_info, "%s\n%s\n" % (ip, ret))

        except:pass
        
def init_plugin(name):
    return http_prob_plugin(name)
