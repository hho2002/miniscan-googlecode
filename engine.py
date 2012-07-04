# -*- coding: gb2312 -*-
import re, struct
import threading
import Queue
import copy
import StringIO
import socket
import time
import random

from node import *
from task import *
from crawler import web_crawler

class cfg_file_parser:
    # class (or static) variable
    key_pattern = re.compile(r'\s*(\w+)\s*=\s*([^#]+)')
    vaule_pattern = re.compile(r'\s*([^#\r\n]+)')
    
    def __init__(self, filename, fp = None):
        self.key_dict = {}
        self.task = ""          # ��Ӧ����������
        key = value = ""
        
        if not fp:
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
        return self.key_dict[key]

class engine(dis_node):
    def __init__(self, ini_file = "node.ini"):
        cfg = cfg_file_parser(ini_file)
        self.thread_idle = set()    # �����߳��б�
        self.queue = None           # ���̵߳���work����
        self.plugins = {}           # ����б� dict key = �����
        self.tasks = {}             # �����б� dict key = task_id
        self.pending_task = []      # ���߽ڵ�������·ַ�������
        self.cfgs = {}              # ÿ������������ļ�    key  = task_name
        self.tasks_ref = {}         # �������ü�����
        self.log_lock = threading.Lock()
        
        # init dis_node
        name = cfg.get_cfg_vaule("node_id")
        try:
            self.max_thread = int(cfg.get_cfg_vaule("maxthread"))
        except:
            self.max_thread = 10
        try:
            port = int(cfg.get_cfg_vaule("node_port"))
        except:
            port = DEFAULT_NODE_PORT
            
        try:
            parent = cfg.get_cfg_vaule("node_parent").split(":")
            parent = (parent[0], int(parent[1]))
        except:
            parent = None
        
        dis_node.__init__(self, name, parent, port)
        
        # start main running thread
        threading.Thread(target=self.__run).start()
        
    def __init_threads(self):
        self.queue = Queue.Queue(self.max_thread * 2)
        for i in xrange(self.max_thread):
            thread = threading.Thread(target=self.__worker_thread,
                                      args = (i,),
                                      name = "thread" + str(i))
            thread.setDaemon(0)
            thread.start()
    
    def __init_plugins(self, cfg):
        plugins = []
        
        try:
            plugins = cfg.get_cfg_vaule("plugin").split(" ")
        except: pass
        
        for plugin_name in set(plugins):
            module = __import__(plugin_name)

            plugin = module.init_plugin(plugin_name)
            plugin.engine = self
            self.plugins[plugin_name] = plugin
            
        return plugins
    
    def __id_to_task(self, _id):
        for task in self.tasks.values():
            try:
                task = task.get_task_by_id(_id)
                return task
            except: pass
            
        raise Exception("Task id %d no found" % _id)
    
    def log(self, task_info, log):
        time_stamp = time.strftime('%Y-%m-%d %X', time.localtime(time.time()))
        task_name = task_info['task']
        
        self.log_lock.acquire()
        dis_node.log(self, self.name + '\t' +       # �ڵ���
                            task_name + '\t' +      # ������
                            time_stamp + '\t' +     # ʱ���
                            task_info['work'][1] + '\t' +   # �����
                            log + '\n')
        self.log_lock.release()
    
    def load_task(self, filename, context=None):
        if context:
            print "load_task:", filename
            cfg = cfg_file_parser(filename, StringIO.StringIO(context))
        else:
            cfg = cfg_file_parser(filename)
        
        task_name = filename.split('.')[0]
        cfg.task = task_name
        self.cfgs[task_name] = cfg
        plugins = self.__init_plugins(cfg)
        
        # �������ϼ���web��hostɨ��
        try:
            task = node_task(task_name, cfg.get_cfg_vaule("host"), plugins)
        except:
            task = web_crawler(task_name, cfg.get_cfg_vaule("webs"), plugins)

        self.tasks_ref[task_name] = 1
        self.tasks[task.id] = task
        for child in self.childs.values():
            if child['name']:
                self.set_node_cfg(child, cfg)
            
    def handler_query(self):
        """ ��ѯ����״̬
        """
        tasks_info = ''
        for task in self.tasks.values():
            task_name = task.name
            if not task.current:
                continue
            
            if isinstance(task, node_task):
                ip = socket.inet_ntoa(struct.pack("L", socket.htonl(task.current)))
                info = "\nNAME:%s\tID:%d\tREMAIN:%d\tREF:%d\nCURRENT: %s\n" % \
                        (task_name, task.id, task.get_task_count(), self.tasks_ref[task_name], ip)
            else:
                url, all_url = task.get_process_info()
                info = "\nNAME:%s\tID:%d\tREMAIN:%d/%d\nCURRENT: %s\n" % \
                        (task_name, task.id, url, all_url, task.current)

            tasks_info += info
            
        if not tasks_info:
            tasks_info = 'NULL TASKS\n'
            
        return tasks_info
    
    def handler_node_log(self, node, msg):
        self.log_lock.acquire()
        dis_node.log(self, msg)
        self.log_lock.release()
    
    def handler_node_close(self, node):
        """ �ڵ���߹ر�
            1��������ӽڵ���ߣ����ýڵ�����Ǩ��
        """
        if self.parent == node:
            # �ڵ���Զ���������
            return
        
        print "handler_node_close:", node['tasks']
        # ��ʱ�����б������·���
        for task_id in node['tasks']:
            task = self.__id_to_task(task_id)
            self.pending_task.append(task)
        
        if node['works']:
            self.pending_task.append(node['works'])
        
    def handler_node_conn(self, node):
        """ �������ø��ڵ�
        """
        self.set_node_cfg(node, self.cfgs)
    
    def handler_node_cfg(self, cfg):
        if isinstance(cfg, dict):
            print "RCV CFGS", len(cfg)
            self.cfgs = cfg
            for _cfg in self.cfgs.values():
                self.__init_plugins(_cfg)
        else:
            print "RCV CFG:", cfg.task, cfg
            self.cfgs[cfg.task] = cfg
            self.__init_plugins(cfg)
            
    def handler_node_task_del(self, task_name):
        print "RCV TASK DEL", task_name
        self.__remove_task(task_name)
    
    def handler_node_task(self, task):
        """ ���ܵ��ڵ�����
        """       
        if isinstance(task, list):
            print "RCV task num:", len(task)
            for work in task:
                if self.queue.full():
                    self.set_node_status("busy", True)

                work_done_evt = None
                if work == task[-1]:
                    work_done_evt = (self.__works_done, None)
                    
                self.queue.put((work[0], work[1], work_done_evt))
            print "RCV task DONE"
        else:
            print "RCV task id:", task.id
            self.tasks_ref[task.name] = 1
            self.tasks[task.id] = task
    
    def handler_node_status(self, node, key, value):
        """ �ڵ�״̬���������仯
        """
        if key == "done_id":
            task = self.__id_to_task(value)
            node['tasks'].remove(task.id)
            self.tasks_ref[task.name] -= 1
            task.done = True
        
        if key == "works" and value == None:
            for task_name, work in node['works']:
                self.tasks_ref[task_name] -= 1
        
    def handler_node_idle(self, node):
        """ ����򵥵�����ַ�
            1��һ�η���һ��ip
        """
        #print "handler_node_idle: tasks " , task.get_task_count()
        if len(self.pending_task) > 0:
            child_task = self.pending_task.pop()
            if isinstance(child_task, list):
                assert node['works'] == None
                node['works'] = child_task
                self.set_node_task(node, child_task)
                return
        else:
            # ����������������ѡһ��ִ���������
            try:
                task = self.tasks.values()[random.randint(0, len(self.tasks) - 1)]
                child_task = task.split(1)
            except: return
        
        self.tasks_ref[child_task.name] += 1
        node['tasks'].add(child_task.id)
        self.set_node_task(node, child_task)
        
    def __try_push_work(self):
        """ �����������Ͷ�������
                                ��ѯ�����ӽڵ�
        """
        for node in self.childs.values():
            if not node['busy'] \
                and not node['works'] \
                and len(node['tasks']) == 0 \
                and node['name']:
                works = []
                while len(works) < self.queue.maxsize:
                    try:
                        # task_id, work
                        task_name, work = self.queue.get(timeout = 1)[:2]
                        works.append((task_name, work))
                        self.tasks_ref[task_name] += 1
                    except:
                        break
                
                node['works'] = works
                self.set_node_task(node, works)
                break
    
    def __works_done(self, obj):
        if isinstance(obj, base_task):
            self.set_node_status("done_id", obj.id)
            self.tasks_ref[obj.name] -= 1
        else:
            self.set_node_status("works", None, force_refresh = True)
    
    def __worker_thread(self, thread_id):
        """ ����ַ��߳�
        """
        while True:
            try:
                task_name, work, work_done_evt = self.queue.get(timeout = 1)
                task, plugin = work
                
                try:
                    self.thread_idle.remove(thread_id)
                except: pass
                
                self.set_node_status("idle", False)
                
                self.plugins[plugin].handle_task({'work':work, 'task':task_name})
                
                if work_done_evt:
                    func, argv = work_done_evt
                    func(argv)

            except Queue.Empty:
                self.thread_idle.add(thread_id)
                
                if len(self.thread_idle) == self.max_thread:
                    if self.set_node_status("idle", True):
                        self.idle_time = time.time()
                    elif time.time() - self.idle_time > 5:
                        self.idle_time = time.time()
                        self.set_node_status("idle", True, force_refresh = True)

    def __remove_task(self, task_name):
        """ ���ڵ����ӽڵ㷢��ɾ��task����
        """
        # remove all tasks resources
        try:
            self.cfgs.pop(task_name)
            print "!!!!remove task:", task_name
            for child in self.childs.values():
                self.del_node_task(child, task_name)
        except: pass
        
    def __run(self):
        self.__init_threads()
        while True:
            if self.status['idle']:                
                # test pending_task
                if len(self.pending_task) > 0:
                    self.handler_node_task(self.pending_task.pop())
                else:
                    time.sleep(0.1)

            for key in self.tasks.keys():
                task = self.tasks[key]
                if self.tasks_ref[task.name] <= 0:
                    if not self.parent:
                        self.__remove_task(task.name)
                    self.tasks.pop(key)
                    continue
                
                if task.done:
                    continue

                try:
                    work = task.move_next()
                    work_done_evt = None
                    
                    if task.get_task_count() == 0:
                        work_done_evt = (self.__works_done, task)
                    
                    self.queue.put((task.name, work, work_done_evt))
                except: pass
                
            # test node busy
            if self.queue.full():
                self.set_node_status("busy", True)
                self.__try_push_work()
            elif self.queue.qsize() < self.queue.maxsize/2:
                self.set_node_status("busy", False)
            
if __name__ == '__main__':
    server = engine()
#    server.load_task("task.txt")
#    server.load_task("task2.txt")
    #server.run()
    #threading.Thread(target=server.run).start()
    
    #child = engine("node2.ini")
    #child.run()
