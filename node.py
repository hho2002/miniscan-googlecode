# -*- coding: gb2312 -*-
import socket, select
import threading, time
import struct, StringIO
import cPickle as pickle
import task
import sys
#
# --- pickle docs ---
# http://www.ibm.com/developerworks/cn/linux/l-pypers/index.html
#

DEFAULT_NODE_PORT = 9910
MSG_HDR_LEN = struct.calcsize("ii")

class dis_node:
    def __init__(self, name, parent=None, port=DEFAULT_NODE_PORT):
        self.lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.lport = port
        self.parent = None
        self.childs = {}
        self.status = {"idle":False, "busy":False}
        self.sock_list = []
        self.name = name
        self.cmds = {"CONN":1, 
                     "TASK":2, 
                     "CFG":4, 
                     "PLUGIN":5, 
                     "STATUS":6,
                     "LOG":7,
                     "CLIENT_ADD":8,
                     "CLIENT_QUERY_TASK":9,
                     "MAX":10}

        if parent:
            self.__connect(parent)
            
        self.server_thread = threading.Thread(target=self.server)
        self.server_thread.start()
        
    def rcv_msg(self, node_info, buf):
        if len(buf) < MSG_HDR_LEN:
            return buf

        hdr = struct.unpack_from("ii", buf)
        if len(buf) < hdr[1]:
            return buf
        
        msg = buf[MSG_HDR_LEN:hdr[1]]
        
        if hdr[0] == self.cmds["LOG"]:
            self.handler_node_log(node_info, pickle.loads(msg))
            
        if hdr[0] == self.cmds["CONN"]:
            node_info['name'] = msg
            self.handler_node_conn(node_info)
            
        if hdr[0] == self.cmds["CFG"]:
            self.handler_node_cfg(pickle.loads(msg))
            
        if hdr[0] == self.cmds["STATUS"]:
            key, value = pickle.loads(msg)
            print "STATUS:", key, value
            
            if key == "idle" and value:
                self.handler_node_idle(node_info)
                
            self.handler_node_status(node_info, key, value)
            node_info[key] = value
            
        if hdr[0] == self.cmds["TASK"]:
            self.handler_node_task(pickle.loads(msg))
        
        if hdr[0] == self.cmds["CLIENT_ADD"]:
            name, context = msg.split('\0')
            self.load_task(name, context)
            
        if hdr[0] == self.cmds["CLIENT_QUERY_TASK"]:
            self.__send_to(node_info, self.handler_query())
        
        buf = buf[hdr[1]:]
        
        return self.rcv_msg(node_info, buf)
    
    def __init_node_info(self, sock, addr, is_child = True):
        node = {}
        node['sock'] = sock
        node['addr'] = addr
        node['is_child'] = is_child
        node['pending_pack'] = None     # 处理tcp拼接组包
        node['busy'] = False            # 节点是否繁忙
        node['idle'] = False            # 节点所有工作线程都空闲状态
        node['tasks'] = set()           # 节点接受的任务列表 TASK_ID集合
        node['works'] = None            # 节点接受的工作列表
        node['name'] = ''
        return node

    def server(self):
        self.lsock.bind(('', self.lport))
        self.lsock.listen(10)
        self.sock_list.append(self.lsock)
        while True:
            infds,outfds,errfds = select.select(self.sock_list, [], self.sock_list, 1)
            
            if self.parent and not self.parent['sock']:
                self.__connect(self.parent['addr'])
                
            if len(infds) > 0:
                if self.lsock in infds:
                    connection, address = self.lsock.accept()
                    self.childs[connection] = self.__init_node_info(connection, address)
                    self.sock_list.append(connection)
                    infds.remove(self.lsock)
                
                for rcv_sock in infds:
                    try:
                        buf = rcv_sock.recv(4096)
                        if not buf:
                            self.__sock_close(rcv_sock)
                            continue
                        
                    except Exception, e:
                        print "socket recv error!", e
                        self.__sock_close(rcv_sock)
                        continue
                    
                    if self.parent and self.parent['sock'] == rcv_sock:
                        node_info = self.parent
                    else:
                        node_info = self.childs[rcv_sock]
                    
                    if node_info['pending_pack']:
                        buf = node_info['pending_pack'] + buf

                    node_info['pending_pack'] = self.rcv_msg(node_info, buf)
                    
            if len(errfds) > 0:
                for err_sock in errfds:
                    self.__sock_close(err_sock)

    def handler_node_log(self, node, msg):
        pass
    def handler_node_conn(self, node):
        pass
    def handler_node_idle(self, node):
        pass
    def handler_node_task(self, task):
        pass
    def handler_node_cfg(self, cfg):
        pass
    def handler_node_close(self, node):
        pass
    def handler_node_status(self, node, key, value):
        pass

    def __send_node_obj(self, node, cmd, obj):
        obj_s = pickle.dumps(obj)
        stream = struct.pack("ii", self.cmds[cmd], MSG_HDR_LEN + len(obj_s))
        stream += obj_s
        #print "send task %d bytes" % len(stream)
        self.__send_to(node, stream)
    
    def log(self, log, filename = "result.log"):
        if self.parent:
            self.__send_node_obj(self.parent, "LOG", log)
        else:
            sys.stdout.write(log)
            # 写入日志文件
            if filename:
                fp = open(filename, 'a')
                fp.write(log)
                fp.close()
                
    def set_node_task(self, node, task):
        self.__send_node_obj(node, "TASK", task)
    
    def set_node_cfg(self, node, cfg):
        self.__send_node_obj(node, "CFG", cfg)
          
    def set_node_status(self, key, value, force_refresh = False):
        """ 通知父节点状态改变 
        """
        try:
            if not force_refresh and self.status[key] == value:
                return False
        except: pass
        
        self.status[key] = value
        if self.parent:
            self.__send_node_obj(self.parent, "STATUS", (key, value))
            
        return True
    
    def __sock_close(self, sock):
        if sock in self.childs.keys():
            node = self.childs.pop(sock)
        elif self.parent['sock'] == sock:
            node = self.parent
        else:
            raise Exception("__sock_close: sock %d no found" % sock)
        
        print "__sock_close: %s" % node['addr'][0]
        
        node['sock'] = None
        self.handler_node_close(node)
        self.sock_list.remove(sock)
        sock.close()

    def __send_to(self, node, stream):
        sock = node['sock']
        if sock:
            try:
                sock.send(stream)
            except socket.error:
                self.__sock_close(sock)
    
    def __connect(self, address):
        self.parent = self.__init_node_info(None, address, is_child = False)
        try:
            sock = socket.create_connection(address, 5)
            self.parent['sock'] = sock
            self.sock_list.append(sock)
        except:
            return

        stream = struct.pack("ii", self.cmds["CONN"], MSG_HDR_LEN + len(self.name))
        stream += self.name  
        self.__send_to(self.parent, stream)
        
#node0 = dis_node("node0")
#node1 = dis_node("i'm child" * 1000, ('localhost', 9910), 9911)
#
#fd = StringIO.StringIO()
##print type(node1.parent)
##childs = pickle.dump(node1.parent, fd)
##print fd.getvalue()
#s = pickle.dumps(node1.cmds)
#print s, len(s)
#
#node1.test()
#
#while True:
#    pass

