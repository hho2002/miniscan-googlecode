# -*- coding: gb2312 -*-
import re, struct
import threading
import socket

class engine_plugin:
    def __init__(self, name):
        self.name = name
        self.engine = None
    
    def log(self, log):
        self.engine.log(log)
    
    def get_cfg_vaule(self, key):
        return self.engine.cfg.get_cfg_vaule(key)
        
    def handle_task(self, task):
        pass
    
class base_task:
    ID = 0
    def __init__(self, plugins):
        if len(plugins) < 1:
            raise Exception("base_task no plugins")
        
        base_task.ID += 1
        self.id = base_task.ID
        self.done = False
        self.current = None
        self.childs = {}
        self.plugin = plugins
        self.current_plugin = 0
        self.node = None        
    def test_all_done(self):
        if not self.done:
            return False
        
        for child in self.childs.values():
            if not child.done:
                return False
        return True
    
    def split(self, count):
        pass
    def handler_next(self):
        pass
    def get_task_count(self):
        pass
    def get_task_by_id(self, _id):
        if _id == self.id:
            return self
        
        if _id in self.childs.keys():
            return self.childs[_id]
        
        raise Exception("Task id no found")
    def move_next(self):
        plugin = self.plugin[self.current_plugin]
        if self.current_plugin == 0:
            try:
                self.current = self.handler_next()
            except:
                self.done = True
                raise Exception("Move the last")
            
        if self.current_plugin < len(self.plugin)-1:
            self.current_plugin += 1
        else:
            self.current_plugin = 0
            
        return (self.current, plugin)
    
class host_seg:
    # class (or static) variable
    ip_pattern = re.compile(r'(\d+.\d+.\d+.\d+)[ \t,-](\d+.\d+.\d+.\d+)')

    def __init__(self, hosts = None):
        offset = 0
        self.ip_seg_list = []
        self.ip_count = 0
        self.current_host = None
        self.current_seg = 0
        if not hosts:
            return
        
        for m in re.finditer(r"[\r\n]+", hosts + '\n'):
            ip = hosts[offset:m.start()]
            seg_math = self.ip_pattern.match(ip)
            
            if seg_math:
                ip1 = struct.unpack("I", socket.inet_aton(seg_math.group(1)))[0]
                ip2 = struct.unpack("I", socket.inet_aton(seg_math.group(2)))[0]
                self.append(socket.ntohl(ip1), socket.ntohl(ip2))
            else:
                ip1 = struct.unpack("I", socket.inet_aton(ip))[0]
                self.append(socket.ntohl(ip1))
                
            offset = m.end()
        
    def append(self, start_ip, end_ip = None):
        if not end_ip:
            end_ip = start_ip
        
        # cool swap :-)
        if end_ip < start_ip:
            start_ip, end_ip = end_ip, start_ip

        if not self.current_host:
            self.current_host = start_ip
            
        self.ip_count += end_ip - start_ip + 1
        self.ip_seg_list.append((start_ip, end_ip))

    def split(self, count):
        seg = host_seg()
        while self.ip_count > 0 and seg.ip_count < count:
            if self.current_seg == len(self.ip_seg_list) - 1:
                if self.ip_count <= 1:
                    raise Exception("split ip_count must > 1")
                
                ip1, ip2 = self.ip_seg_list[self.current_seg]
                ip = ip2 - (count - seg.ip_count - 1)
                if ip < self.current_host:
                    ip = self.current_host + 1
                
                self.ip_seg_list[self.current_seg] = (ip1, ip - 1)
                ip1 = ip
            else:
                ip1, ip2 = self.ip_seg_list.pop()
            
            self.ip_count -= ip2 - ip1 + 1
            seg.append(ip1, ip2)
            #print seg.ip_count, self.ip_count
            
        return seg
    
    def move_next(self):
        if self.ip_count <= 0:
            raise Exception("task empty")
        
        start_ip, end_ip = self.ip_seg_list[self.current_seg]
        ret = self.current_host
        
        if self.current_host < end_ip:
            self.current_host += 1
        elif self.current_seg < len(self.ip_seg_list) - 1:
            self.current_seg += 1
            self.current_host = self.ip_seg_list[self.current_seg][0]
        
        self.ip_count -= 1
        return ret

class node_task(base_task):
    def __init__(self, hosts = None, plugins = None):
        base_task.__init__(self, plugins)
        
        if isinstance(hosts, host_seg):
            self.hosts = hosts
        else:
            self.hosts = host_seg(hosts)
   
    def get_task_count(self):        
        ret = len(self.plugin) * self.hosts.ip_count - self.current_plugin 
        if self.current_plugin != 0:
            ret += len(self.plugin)
            
        return ret
    
    def handler_next(self):
        return self.hosts.move_next()
    
    def split(self, count):
        """ 粗略任务切分
            1、优先ip切割
            2、无法ip切割时，切割插件
        """
        
        count = min(self.get_task_count() - 1, count)
        if count < 1:
            raise Exception("split task_count failed")
        
        if self.hosts.ip_count > 1:
            host = self.hosts.split(max(count/len(self.plugin), 1))
            child = node_task(host, self.plugin)
        else:
            pos = len(self.plugin) - count
            if self.get_task_count() < count:
                pos = self.current_plugin + 1
                
            plugin = self.plugin[pos:]
            self.plugin = self.plugin[:pos]

            ip = socket.inet_ntoa(struct.pack("I", socket.htonl(self.hosts.current_host)))
            child = node_task(ip, plugin)
            
        self.childs[child.id] = child
        return child

#host1 = host_seg("192.168.1.1-192.168.1.3\n192.168.1.4")
#host2 = host1.split(2)
#
#print host1.ip_count, host1.ip_seg_list
#print host2.ip_count, host2.ip_seg_list

#plugins = ["plugin1", "plugin2", "plugin3"]
#task1 = node_task("192.168.1.1-192.168.1.3\n192.168.1.4", plugins)
#task2 = task1.split(1)
#task3 = task1.split(1)
#
#print task1.get_task_by_id(task2.id), task2
#
#while True:
#    try:
#        print task1.move_next()
#    except:
#        break
#    
#print "Task2", task2.get_task_count()
#
#while True:
#    try:
#        print task2.move_next()
#    except:
#        break
#    
#print "Task3", task3.get_task_count()
#
#while True:
#    try:
#        print task3.move_next()
#    except:
#        break