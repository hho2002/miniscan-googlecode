# -*- coding: gb2312 -*-
import re, struct
import threading
import Queue
import copy

from socket import *
from node import *
from task import *

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

class engine(dis_node):
    def __init__(self, ini_file = "setting.txt"):
        self.cfg = cfg_file_parser(ini_file)
        self.max_thread = 10
        self.thread_pool = []
        self.queue = None
        self.plugins = {}
        self.tasks = {}
        
        # init dis_node
        name = self.cfg.get_cfg_vaule("node_id")
        port = self.cfg.get_cfg_vaule("node_port")
        if not port:
            port = DEFAULT_NODE_PORT
        else:
            port = int(port)
            
        parent = self.cfg.get_cfg_vaule("node_parent")
        if parent:
            parent = parent.split(":")
            parent = (parent[0], int(parent[1]))
            
        dis_node.__init__(self, name, parent, port)
        
    def init_threads(self):
        self.queue = Queue.Queue(self.max_thread)
        for i in xrange(self.max_thread):
            thread = threading.Thread(target=self.worker_thread,
                                      name = "thread" + str(i))
            self.thread_pool.append(thread)
            thread.setDaemon(0)
            thread.start()
            
    def __init_plugins(self, cfg):
        plugins = cfg.get_cfg_vaule("plugin").split(" ")
        for plugin_name in plugins:
            module = __import__(plugin_name)

            plugin = module.init_plugin(plugin_name)
            plugin.engine = self
            self.plugins[plugin_name] = plugin
        
        return plugins
    
    def load_task(self, filename):
        cfg = cfg_file_parser(filename)
        
        self.cfg = cfg
        self.max_thread = int(cfg.get_cfg_vaule("maxthread"))
        
        plugins = self.__init_plugins(cfg)
        
        task = node_task(cfg.get_cfg_vaule("host"), plugins)
        self.tasks["task0"] = task
        
    def handler_node_conn(self, node):
        """ 发送配置给节点
        """
        self.set_node_cfg(node, self.cfg)
    
    def handler_node_cfg(self, cfg):
        self.cfg = cfg
        self.__init_plugins(cfg)
    
    def handler_node_task(self, task):
        """ 接受到节点任务
        """       
        self.tasks[task.id] = task
        pass
    
    def handler_node_idle(self, node):
        """ 处理简单的任务分发
            1、一次分配一段ip
        """
        
        task = self.tasks["task0"]
        print "handler_node_idle: tasks " , task.get_task_count()
        child_task = task.split(1)
        self.set_node_task(node, child_task)
    
    def worker_thread(self):
        while True:
            try:
                ip, plugin = self.queue.get(timeout = 2)
                # do task !!!
                self.plugins[plugin].handle_task(ip)
            except Queue.Empty:
                if not self.parent:
                    break
    
    def run(self):
        self.init_threads()
        
        while True:
            for task in self.tasks.values():
                if task.get_task_count() == 0:
                    continue
                
                self.queue.put(task.move_next())
                
            if self.parent and self.queue.empty():
                self.set_node_idle()
                continue
        
        #wait for all thread exit
        for thead in self.thread_pool:
            threading.Thread.join(thead)
            
server = engine()
server.load_task("setting.txt")

child = engine("setting2.txt")
child.run()

while True:
    pass