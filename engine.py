# -*- coding: gb2312 -*-
import re, struct
import threading
import Queue
import copy

from socket import *
from node import *
from task import *
from crawler import web_crawler

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
    def __init__(self, ini_file = "node.ini"):
        self.cfg = cfg_file_parser(ini_file)
        self.thread_pool = []
        self.thread_idle = set()    # 空闲线程列表
        self.queue = None
        self.plugins = {}           # 插件列表 dict key = 插件名
        self.tasks = {}             # 任务列表 dict key = task_name
        self.pending_task = set()   # 断线节点的需重新分发的任务
        self.done = False
        self.log_lock = threading.Lock()
        
        try:
            self.max_thread = int(self.cfg.get_cfg_vaule("maxthread"))
        except:
            self.max_thread = 10
        
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
        
    def __init_threads(self):
        self.queue = Queue.Queue(self.max_thread * 2)
        for i in xrange(self.max_thread):
            thread = threading.Thread(target=self.worker_thread,
                                      args = (i,),
                                      name = "thread" + str(i))
            self.thread_pool.append(thread)
            thread.setDaemon(0)
            thread.start()
    
    def __init_plugins(self, cfg):
        web_plugins = plugins = []
        
        try:
            plugins = cfg.get_cfg_vaule("plugin").split(" ")
            web_plugins = cfg.get_cfg_vaule("web_plugin").split(" ")
        except: pass
        
        for plugin_name in set(plugins + web_plugins):
            module = __import__(plugin_name)

            plugin = module.init_plugin(plugin_name)
            plugin.engine = self
            self.plugins[plugin_name] = plugin
            
        return (plugins, web_plugins)
    
    def __id_to_task(self, _id):
        for task in self.tasks.values():
            try:
                task = task.get_task_by_id(_id)
                return task
            except: pass
            
        raise Exception("Task id %d no found" % _id)
    
    def log(self, log):
        self.log_lock.acquire()
        dis_node.log(self, log)
        self.log_lock.release()
    
    def load_task(self, filename):
        cfg = cfg_file_parser(filename)
        
        self.cfg = cfg
        
        plugins = self.__init_plugins(cfg)
        
        if len(plugins[0]) > 0:
            task = node_task(cfg.get_cfg_vaule("host"), plugins[0])
            self.tasks['task0'] = task
        
        if len(plugins[1]) > 0:
            web_task = web_crawler(cfg.get_cfg_vaule("web_host"), plugins[1])
            self.tasks['task1'] = web_task
            
    def handler_node_log(self, node, msg):
        self.log(node['addr'][0] + "\t" + msg)
    
    def handler_node_close(self, node):
        """ 节点断线关闭
            1、如果是子节点断线，将该节点任务迁移
        """
        if self.parent == node:
            # 节点会自动处理重连
            return
        
        print "handler_node_close:", node['tasks']
        # 临时放入列表，待重新分配
        for task_id in node['tasks']:
            task = self.__id_to_task(task_id)
            self.pending_task.add(task)
        
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
        if isinstance(task, list):
            print "RCV task:", len(task)
            for work in task:
                if self.queue.full():
                    self.set_node_status("busy", True)

                self.queue.put(work)
            print "RCV task DONE"
        else:
            print "RCV task obj:", task.id
            self.tasks[task.id] = task
    
    def handler_node_status(self, node, key):
        """ 节点状态发生变化
        """
        if key == "done_id":
            task = self.__id_to_task(node[key])
            node['tasks'].remove(task.id)
            task.done = True
    
    def handler_node_idle(self, node):
        """ 处理简单的任务分发
            1、一次分配一段ip
        """
        #print "handler_node_idle: tasks " , task.get_task_count()
        if len(self.pending_task) > 0:
            child_task = self.pending_task.pop()
        else:
            task = self.tasks['task0']
            try:
                child_task = task.split(1)
            except: return
        
        node['tasks'].add(child_task.id)
        self.set_node_task(node, child_task)
        
    def __try_push_work(self):
        """ 尝试主动推送队列任务
                                查询空闲子节点
        """
        for node in self.childs.values():
            if not node['busy']:
                work = []
                while len(work) < self.queue.maxsize:
                    try:
                        work.append(self.queue.get(timeout = 1))
                    except:
                        break
                    
                self.set_node_task(node, work)
    
    def worker_thread(self, thread_id):
        """ 任务分发线程
        """
        while True:
            try:
                task, plugin = self.queue.get(timeout = 1)
                
                try:
                    self.thread_idle.remove(thread_id)
                except: pass
                
                self.set_node_status("work_done", False)
                
                task = (self.name, task)
                self.plugins[plugin].handle_task(task)
            except Queue.Empty:
                self.thread_idle.add(thread_id)
                
                if len(self.thread_idle) == self.max_thread:
                    self.set_node_status("work_done", True)
                
                if not self.parent and self.done:
                    break
        
        #print threading.current_thread().name, " worker exiting !!!"
        
    def run(self):
        self.__init_threads()
        self.done = False
        while True:
            all_done = self.status["work_done"]
            
            # test pending_task
            if all_done and len(self.pending_task) > 0:
                pending_task = self.pending_task.pop()
                self.tasks[pending_task.id] = pending_task
                all_done = False
                
            for task in self.tasks.values():
                if not task.test_all_done():
                    all_done = False
                    
                if task.done:
                    continue

                try:
                    self.queue.put(task.move_next())
                except Exception, e:
                    print "task %d done! %s" % (task.id, e)
                    self.set_node_status("done_id", task.id)
                    
            # test node busy
            if self.queue.full():
                self.set_node_status("busy", True)
                self.__try_push_work()
                #time.sleep(2)
            elif self.queue.qsize() < self.queue.maxsize/2:
                self.set_node_status("busy", False)
                
            if self.parent and self.queue.empty():
                self.set_node_idle()
                time.sleep(2)
                continue
            
            #test for child work done
            for child in self.childs.values():
                if not child["work_done"]:
                    all_done = False
                    break

            if len(self.thread_idle) == self.max_thread and all_done:
                break
            
        self.done = True
        # print self.pending_task, self.status["work_done"]
        self.log(self.name + " main scan thread exiting !!!")
        #wait for all thread exit
        for thead in self.thread_pool:
            threading.Thread.join(thead)
            
server = engine()
server.load_task("task.txt")
threading.Thread(target=server.run).start()

child = engine("node2.ini")
child.run()

while True:
    pass