# -*- coding: gb2312 -*-
import re, struct
import threading
import Queue
import copy

from socket import *
#from node import *

class engine_plugin:
    def __init__(self, name):
        self.name = name
        self.engine = None
        self.log_lock = threading.Lock()
    
    def log(self, log):
        self.log_lock.acquire()
        print log
        self.log_lock.release()
    
    def get_cfg_vaule(self, key):
        return self.engine.cfg.get_cfg_vaule(key)
        
    def handle_task(self, task):
        pass
    
class cfg_file_parser:
    # class (or static) variable
    key_pattern = re.compile(r'\s*(\w+)\s*=\s*([^#]+)')
    vaule_pattern = re.compile(r'\s*([^#\r\n]+)')
    
    def __init__(self, filename):
        self.key_dict = {}
        key = value = ""
        fp = open(filename, 'r')
        
        while True:
            buf = fp.readline()
            if not buf:
                break
            
            match = self.key_pattern.match(buf)
            if match:
                if key:
                    self.key_dict[key] = value.strip()
                    
                key = match.group(1)
                value = match.group(2)
                
                self.key_dict[key] = value.strip()
            elif key:
                match = self.vaule_pattern.match(buf)
                if match:
                    value += match.group(1) + '\n'

        if key:
            self.key_dict[key] = value.strip()
            
        fp.close()
        
    def get_cfg_vaule(self, key):
        try:
            return self.key_dict[key]
        except:
            return None
        
class host_seg:
    # class (or static) variable
    ip_pattern = re.compile(r'(\d+.\d+.\d+.\d+)[ \t,-](\d+.\d+.\d+.\d+)')

    def __init__(self, hosts = None):
        offset = 0
        self.ip_seg_list = []
        self.ip_count = 0
        self.current = None
        self.current_seg = 0
        if not hosts:
            return
        
        for m in re.finditer(r"[\r\n]+", hosts + '\n'):
            ip = hosts[offset:m.start()]
            seg_math = self.ip_pattern.match(ip)
            
            if seg_math:
                ip1 = struct.unpack("I", inet_aton(seg_math.group(1)))[0]
                ip2 = struct.unpack("I", inet_aton(seg_math.group(2)))[0]
                self.append(ntohl(ip1), ntohl(ip2))
            else:
                ip1 = struct.unpack("I", inet_aton(ip))[0]
                self.append(ntohl(ip1))
                
            offset = m.end()
        
    def append(self, start_ip, end_ip = None):
        if not end_ip:
            end_ip = start_ip
        
        # cool swap :-)
        if end_ip < start_ip:
            start_ip, end_ip = end_ip, start_ip

        if not self.current:
            self.current = start_ip
            
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
                if ip < self.current:
                    ip = self.current + 1
                
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
            return None
        
        start_ip, end_ip = self.ip_seg_list[self.current_seg]
        ret = self.current
        
        if self.current < end_ip:
            self.current += 1
        elif self.current_seg < len(self.ip_seg_list) - 1:
            self.current_seg += 1
            self.current = self.ip_seg_list[self.current_seg][0]
        
        self.ip_count -= 1
        return ret

class node_task:
    def __init__(self, hosts = None, plugins = None):
        if isinstance(hosts, host_seg):
            self.hosts = hosts
        else:
            self.hosts = host_seg(hosts)

        self.plugin = []
        
        if plugins:
            for p in plugins:
                self.plugin.append(p)

        self.id = 0
        self.current_plugin = 0
        self.current_host = None
        
    def get_task_count(self):        
        ret = len(self.plugin) * self.hosts.ip_count - self.current_plugin 
        if self.current_plugin != 0:
            ret += len(self.plugin)
            
        return ret
    
    def move_next(self):
        plugin = self.plugin[self.current_plugin]
        if self.current_plugin == 0:
            self.current_host = self.hosts.move_next()
            if self.current_host == None:
                raise Exception("task empty")
        
        if self.current_plugin < len(self.plugin)-1:
            self.current_plugin += 1
        else:
            self.current_plugin = 0
            
        return (self.current_host, plugin)
    
    def split(self, count):
        """ 粗略任务切分
            1、优先ip切割
            2、无法ip切割时，切割插件
        """
        
        count = min(self.get_task_count() - 1, count)
        
        if self.hosts.ip_count > 1:
            host = self.hosts.split(max(count/len(self.plugin), 1))
            return node_task(host, self.plugin)
        else:
            pos = len(self.plugin) - count
            if self.get_task_count() < count:
                pos = self.current_plugin + 1

            plugin = self.plugin[pos:]
            self.plugin = self.plugin[:pos]

            ip =  inet_ntoa(struct.pack("I", htonl(self.hosts.current)))
            
            return node_task(ip, plugin)

host1 = host_seg("192.168.1.1-192.168.1.3\n192.168.1.4")
host2 = host1.split(2)

print host1.ip_count, host1.ip_seg_list
print host2.ip_count, host2.ip_seg_list

plugins = ["plugin1", "plugin2", "plugin3"]
task1 = node_task("192.168.1.1-192.168.1.1", plugins)

task2 = task1.split(1)

while True:
    try:
        print task1.move_next()
    except:
        break
    
print "Task2"

while True:
    try:
        print task2.move_next()
    except:
        break
    

class engine:
    def __init__(self):
        self.cfg = None
        self.max_thread = 10
        self.host_seg = None
        self.thread_pool = []
        self.queue = None
        self.plugins = []
        self.tasks = {}
        # init node 
        
    def init_threads(self):
        self.queue = Queue.Queue(self.max_thread)
        for i in xrange(self.max_thread):
            thread = threading.Thread(target=self.worker_thread,
                                      name = "thread" + str(i))
            self.thread_pool.append(thread)
            thread.setDaemon(0)
            thread.start()
        
    def load_task(self, filename):
        cfg = cfg_file_parser(filename)
        self.cfg = cfg
        self.host_seg = host_seg(cfg.get_cfg_vaule("host"))
        self.max_thread = int(cfg.get_cfg_vaule("maxthread"))
        
        # init plugin
        plugin_name = cfg.get_cfg_vaule("plugin")
        module = __import__(plugin_name)
        
        if module.init_plugin:
            plugin = module.init_plugin(plugin_name)
            plugin.engine = self
            self.plugins.append(plugin)
            
    def handler_node_idle(self, node, task):
        
        pass 
    
    def worker_thread(self):
        while True:
            try:
                ip = self.queue.get(timeout = 2)
                # do task !!!
                for plugin in self.plugins:
                    plugin.handle_task(ip)
            except Queue.Empty:
                break
    
    def run(self):
        self.init_threads()
        
        while True:
            ip = self.host_seg.move_next()
            if ip == None:
                break;
            
            self.queue.put(ip)
        
        #wait for all thread exit
        for thead in self.thread_pool:
            threading.Thread.join(thead)