# -*- coding: gb2312 -*-
import socket, select
import threading
import struct
import cPickle as pickle
import zlib
import sys
#
# --- pickle docs ---
# http://www.ibm.com/developerworks/cn/linux/l-pypers/index.html
#

DEFAULT_NODE_PORT = 9910
MSG_HDR_LEN = struct.calcsize("ii")
MSG_COMPRESS_FLAG = 0x10000

class dis_node:
    def __init__(self, name, parent=None, port=DEFAULT_NODE_PORT):
        self.lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.lport = port
        self.parent = None
        self.nodes = {}
        self.status = {"idle":False, "busy":False}
        self.sock_list = []
        self.name = name
        self.cmds = {"CONN":1, 
                     "TASK":2, 
                     "DEL_TASK":3,
                     "MSG":4, 
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
        
    def __rcv_msg(self, node, buf):
        if len(buf) < MSG_HDR_LEN:
            return buf

        msg_type, msg_len = struct.unpack_from("ii", buf)
        if len(buf) < msg_len:
            return buf
        
        msg = buf[MSG_HDR_LEN:msg_len]
        if msg_type & MSG_COMPRESS_FLAG:
            msg = zlib.decompress(msg)
            msg_type -= MSG_COMPRESS_FLAG
            
        if msg_type == self.cmds["LOG"]:
            self.handler_node_log(node, pickle.loads(msg))
            
        if msg_type == self.cmds["CONN"]:
            node['name'] = msg
            self.__send_node_obj(node, "STATUS", ('name', self.name))
            self.handler_node_conn(node)
            
        if msg_type == self.cmds["MSG"]:
            _msg, _obj = pickle.loads(msg)
            if not self.handler_node_msg(node, _msg, _obj):
                for _node in self.nodes.values():
                    if _node['name'] and _node != node:
                        self.__send_to(_node, buf[:msg_len])
            
        if msg_type == self.cmds["STATUS"]:
            key, value = pickle.loads(msg)
            print "STATUS:", key, value
            
            if key == "idle" and value:
                self.handler_node_idle(node)
                
            self.handler_node_status(node, key, value)
            node[key] = value
            
        if msg_type == self.cmds["TASK"]:
            self.handler_node_task(node, pickle.loads(msg))
        if msg_type == self.cmds["DEL_TASK"]:
            self.handler_node_task_del(node, pickle.loads(msg))
        if msg_type == self.cmds["CLIENT_ADD"]:
            name, context = msg.split('\0')
            self.load_task(name, context)
            
        if msg_type == self.cmds["CLIENT_QUERY_TASK"]:
            self.__send_to(node, self.handler_query())
        
        buf = buf[msg_len:]
        
        return self.__rcv_msg(node, buf)
    
    def __init_node_info(self, sock, addr):
        node = {}
        node['sock'] = sock
        node['addr'] = addr
        node['pending_pack'] = None     # 处理tcp拼接组包
        node['busy'] = False            # 节点是否繁忙
        node['idle'] = False            # 节点所有工作线程都空闲状态
        node['tasks'] = set()           # 节点接受的任务列表 TASK_ID集合
        node['works'] = None            # 节点接受的工作列表
        node['task_count'] = 0          # 节点剩余任务量，优先调度最大任务量
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
                    self.nodes[connection] = self.__init_node_info(connection, address)
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
                        node_info = self.nodes[rcv_sock]
                    
                    if node_info['pending_pack']:
                        buf = node_info['pending_pack'] + buf

                    node_info['pending_pack'] = self.__rcv_msg(node_info, buf)
                    
            if len(errfds) > 0:
                for err_sock in errfds:
                    self.__sock_close(err_sock)

    def handler_node_log(self, node, msg):
        pass
    def handler_node_conn(self, node):
        pass
    def handler_node_idle(self, node):
        pass
    def handler_node_task(self, node, task):
        pass
    def handler_node_task_del(self, node, task_name):
        pass
    def handler_node_msg(self, node, msg, obj):
        """ 处理节点接受到的消息，返回True表示该消息处理完毕，Flase会继续转发
        """
        pass
    def handler_node_close(self, node):
        pass
    def handler_node_status(self, node, key, value):
        pass
    def __send_node_obj(self, node, cmd, obj):
        msg_type = self.cmds[cmd]
        obj_s = pickle.dumps(obj)
        if len(obj_s) > 0x100:
            obj_s = zlib.compress(obj_s)
            msg_type |= MSG_COMPRESS_FLAG
            
        stream = struct.pack("ii", msg_type, MSG_HDR_LEN + len(obj_s))
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

    def del_node_task(self, node, name):
        self.__send_node_obj(node, "DEL_TASK", name)
    def set_node_task(self, node, task):
        self.__send_node_obj(node, "TASK", task)
    def send_msg(self, msg, obj, target=None):
        if target:
            self.__send_node_obj(target, "MSG", (msg, obj))
            return
        
        for node in self.nodes.values():
            if not node['name']:
                continue
            self.__send_node_obj(node, "MSG", (msg, obj))
    def set_node_status(self, key, value, target=None, force_refresh=False):
        """ 通知节点状态改变 
        """
        try:
            if not force_refresh and self.status[key] == value:
                return False
        except: pass
        
        self.status[key] = value
        if target:
            self.__send_node_obj(target, "STATUS", (key, value))
        else:
            for node in self.nodes.values():
                self.__send_node_obj(node, "STATUS", (key, value))
                
        return True
    
    def __sock_close(self, sock):
        if sock in self.nodes.keys():
            node = self.nodes.pop(sock)
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
        self.parent = self.__init_node_info(None, address)
        try:
            sock = socket.create_connection(address, 5)
            self.parent['sock'] = sock
            self.sock_list.append(sock)
        except:
            return

        self.nodes[sock] = self.parent
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

