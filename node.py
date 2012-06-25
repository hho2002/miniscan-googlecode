import socket, select
import threading, time
import struct, StringIO
import cPickle as pickle
from engine import *

#
# --- pickle docs ---
# http://www.ibm.com/developerworks/cn/linux/l-pypers/index.html
#

DEFAULT_NODE_PORT = 9910
  
class node:
    def __init__(self, name, parent=None, port=DEFAULT_NODE_PORT):
        self.lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.lport = port
        self.parent = None
        self.childs = {}
        self.sock_list = []
        self.name = name
        self.cmds = {"CONN":1, "TASK":2, "EXIT":3, "MAX":6}

        if parent:
            self.connect(parent)
            
        self.server_thread = threading.Thread(target=self.server)
        self.server_thread.start()
        
    def rcv_msg(self, node_info, buf):
        hdr_len = struct.calcsize("ii")
        if len(buf) < hdr_len:
            return buf

        hdr = struct.unpack_from("ii", buf)
        if len(buf) < hdr[1]:
            return buf
        
        if hdr[0] == self.cmds["CONN"]:
            name = buf[hdr_len:hdr[1]]
            buf = buf[hdr[1]:]
            print name, buf
            
            if node_info["is_child"]:
                msg = self.name
                stream = struct.pack("ii", self.cmds["CONN"], hdr_len + len(msg))
                stream += msg
                node_info["sock"].send(stream)

        return self.rcv_msg(node_info, buf)
    
    def server(self):
        self.lsock.bind(('localhost', self.lport))
        self.lsock.listen(10)
        self.sock_list.append(self.lsock)
        while True:
            infds,outfds,errfds = select.select(self.sock_list, [], self.sock_list, 1)
            
            if len(infds) > 0:
                if self.lsock in infds:
                    connection, address = self.lsock.accept()
                    child = {}
                    child['sock'] = connection
                    child['addr'] = address
                    child['pending_pack'] = None
                    child['is_child'] = True
                    self.childs[connection] = child
                    self.sock_list.append(connection)
                    infds.remove(self.lsock)
                
                for rcv_sock in infds:
                    try:
                        buf = rcv_sock.recv(4096)
                    except Exception, e:
                        print "socket recv error!", e
                        self.sock_list.remove(rcv_sock)
                        continue
                    
                    if self.parent and self.parent["sock"] == rcv_sock:
                        node_info = self.parent
                    else:
                        node_info = self.childs[rcv_sock]
                    
                    if node_info['pending_pack']:
                        buf = node_info['pending_pack'] + buf

                    node_info['pending_pack'] = self.rcv_msg(node_info, buf)
                    
            if len(errfds) > 0:
                pass
   
    def handler_node_idle(self, node, task):
        pass
    
    def handler_node_task(self, task):
        pass
    
    def set_node_task(self, node, task):
        s = pickle.dumps(task)
        print s, len(s)
        stream = struct.pack("ii", 
                             self.cmds["TASK"], 
                             struct.calcsize("ii") + len(self.name))
        
        pass
    
    def set_node_idle(self):
        # request for tasks
        if not self.parent:
            return
        
        pass
    
    def connect(self, address):
        parent = {}
        sock = socket.create_connection(address, 5)
        parent['sock'] = sock
        parent['addr'] = address
        parent['is_child'] = False
        parent['pending_pack'] = None
        # msg header
        stream = struct.pack("ii",  
                             self.cmds["CONN"], 
                             struct.calcsize("ii") + len(self.name))
        
        stream += self.name
        
        sock.send(stream)
        
        self.parent = parent
        self.sock_list.append(sock)

    def close(self):
        pass
    
    def test(self):
        fd = StringIO.StringIO()
        pickle.dump(self.parent, fd)
        print fd.getvalue()
        pass
    
node0 = node("node0")
node1 = node("i'm child" * 1000, ('localhost', 9910), 9911)

fd = StringIO.StringIO()
#print type(node1.parent)
#childs = pickle.dump(node1.parent, fd)
#print fd.getvalue()
s = pickle.dumps(node1.cmds)
print s, len(s)

node1.test()

while True:
    pass

