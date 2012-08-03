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
import web_server

class cfg_file_parser:
    # class (or static) variable
    key_pattern = re.compile(r'\s*(\w+)\s*=\s*([^#]+)')
    vaule_pattern = re.compile(r'\s*([^#\r\n]+)')
    
    def __init__(self, filename, fp = None):
        self.key_dict = {}
        self.task = ""          # 对应的任务名称
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

class dispatch_work:
    def __init__(self, task_name, work, done_evt = None):
        self.task_name = task_name
        self.id_task = 0
        self.work_group = 0
        self.current = work[0]      # base_task.current
        self.plugin = work[1]       # plugin_name
        self.process = work[2]      # plugin_process
        self.done_evt = done_evt    # func, argv

class engine(dis_node):
    FLAG_PENDING_DEL = 1
    def __init__(self, ini_file = "node.ini"):
        cfg = cfg_file_parser(ini_file)
        self.thread_idle = set()    # 空闲线程列表
        self.queue = None           # 多线程调度work队列
        self.plugins = {}           # 插件列表 dict key = 插件名
        self.tasks = {}             # 任务列表 dict key = task_id
        self.pending_task = []      # 断线节点的需重新分发的任务
        self.cfgs = {}              # 每个任务的配置文件    key  = task_name
        self.tasks_ref = {}         # 任务引用计数器 key = id(task)
        self.works_ref = {}         # key = 组ID value = (ref, node)
        self.tasks_status = {}      # key = task_name value "pause" "run"
        self.group_id = 0
        self.ref_lock = threading.Lock()
        self.idle_time = time.time()
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
        
        # start web_deamon
        try:
            if cfg.get_cfg_vaule("web_deamon").lower() == "true":
                web_server.run_server(self, 80)
        except: pass
        
    def __init_threads(self):
        self.queue = Queue.Queue(self.max_thread * 4)
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
    
    def __id_to_child_task(self, _id):
        for task in self.tasks.values():
            try:
                child = task.get_child_by_id(_id)
                return (child, task)
            except: pass
            
        raise Exception("Task id %d no found" % _id)
    
    def get_node_task_count(self):
        node_task_count = 0
        for task in self.tasks.values():
            node_task_count += task.get_task_count()
        return node_task_count
    
    def __add_ref(self, task, value):
        """ 增加任务引用 task为任务或者ID
        """
        try:
            if isinstance(task, int):
                id_task = task
            else:
                id_task = id(task)
            self.ref_lock.acquire()
            self.tasks_ref[id_task] += value
        except:
            self.tasks_ref[id_task] = value
        finally:
            self.ref_lock.release()
        
    def __add_work_ref(self, work, value):
        if work.work_group:
            self.ref_lock.acquire()
            self.works_ref[work.work_group][0] += value
            self.ref_lock.release()
            if self.works_ref[work.work_group][0] <= 0:
                node = self.works_ref[work.work_group][1]
                print node['name'], "->", self.name, "works complete"
                self.set_node_status("works", None, target=node, force_refresh = True)
                self.works_ref.pop(work.work_group)
                
    def log(self, task_info, log):
        time_stamp = time.strftime('%Y-%m-%d %X', time.localtime(time.time()))
        task_name = task_info['task']
        
        log_info = {'node':self.name, \
                    'task':task_name, \
                    'time':time_stamp, \
                    'plugin':task_info['plugin']}
        
        if self.parent:
            self.send_msg("LOG", (log_info, log), target = self.parent)
        else:
            self.plugins[task_info['plugin']].handle_log(log_info, log)
        
    def set_task_status(self, task_name, status):
        self.tasks_status[task_name] = status
        if status == "del":
            self.__remove_task(task_name)
        self.send_msg("TASK_STATUS", (task_name, status))
        
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
        
        # 不允许混合加载web和host扫描
        if 'max_id' in cfg.key_dict.keys():
            task = id_task(task_name, int(cfg.get_cfg_vaule("max_id")), plugins)
        elif 'host' in cfg.key_dict.keys():
            task = node_task(task_name, cfg.get_cfg_vaule("host"), plugins)
        else:
            task = web_crawler(task_name, cfg.get_cfg_vaule("webs"), plugins)

        task.init_plugin_process(self.plugins)
        
        self.set_task_status(task_name, "run")
        self.send_msg("CFG", cfg)
        
        self.__add_ref(task, 1)
        self.tasks[task.id] = task

    def handler_client_control(self, node, cmd, msg):
        if cmd in ("pause", "run", "del"):
            self.set_task_status(msg, cmd)
            
    def handler_web_query(self, username):
        """ 返回特定格式的web数据格式
        """
        web_tasks = {}
        for task in self.tasks.values():
            #'task1':{'status':"run", 
            #'id':1, 'ref':1, 'log_count':10, 'remain':30, 'current':"172.16.16.1"},
            item = web_tasks[task.name] = {}
            item['status'] = self.tasks_status[task.name]
            item['id'] = task.id
            item['ref'] = self.tasks_ref[id(task)]
            item['log_count'] = 1
            item['remain'] = task.get_task_count()
            item['current'] = socket.inet_ntoa(struct.pack("L", socket.htonl(task.current)))
        return web_tasks
    
    def handler_client_query(self):
        """ 查询任务状态
        """
        tasks_info = 'TASK NUM:%d\n' % len(self.tasks)
        for task in self.tasks.values():
            task_name = task.name
            if not task.current:
                continue
            
            if isinstance(task, node_task):
                ip = socket.inet_ntoa(struct.pack("L", socket.htonl(task.current)))
                info = "\nNAME:%s\tID:%d\tREMAIN:%d\tREF:%d\tSTATUS:%s\nCURRENT: %s\n" % \
                        (task_name, task.id, task.get_task_count(), self.tasks_ref[id(task)],
                         self.tasks_status[task_name], ip)
            elif isinstance(task, web_crawler):
                url, all_url = task.get_process_info()
                info = "\nNAME:%s\tID:%d\tREMAIN:%d/%d\nCURRENT: %s\n" % \
                        (task_name, task.id, url, all_url, task.current)
            else:
                # ID TASK
                pass
            
            tasks_info += info
            
        return tasks_info
    
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
            task = self.__id_to_child_task(task_id)[0]
            self.pending_task.append(task)
        
        if node['works']:
            self.pending_task.append(node['works'])
        
    def handler_node_conn(self, node):
        """ 发送配置给节点
        """
        if len(self.cfgs) > 0:
            self.send_msg("CFGS", self.cfgs, target=node)
            
    def handler_node_msg(self, node, msg, obj):
        """ 接受到节点消息，返回值FALSE表示继续转发消息
        """
        if msg == "CFGS":
            print "RCV CFGS", len(obj)
            self.cfgs = obj
            for _cfg in self.cfgs.values():
                self.__init_plugins(_cfg)
        if msg == "CFG":
            cfg = obj
            print "RCV CFG:", cfg.task, cfg
            self.cfgs[cfg.task] = cfg
            self.__init_plugins(cfg)
        if msg == "TASK_STATUS":
            task_name, status = obj
            print "TASK_STATUS:", task_name, status
            self.tasks_status[task_name] = status
            if status == "del":
                self.__remove_task(task_name)
        if msg == "LOG":
            log_info, log = obj
            if not self.parent:
                self.send_msg(msg, obj, target = self.parent)
            else:
                self.plugins[log_info['plugin']].handle_log(log_info, log)
                
            return True
        
        return False
    
    def handler_node_task(self, node, task):
        """ 接受到节点任务
        """
        if isinstance(task, list):
            print "%s -> %s task num:%d" % (node['name'], self.name, len(task))
            
            self.group_id += 1
            group_id = self.group_id
            self.works_ref[group_id] = [len(task), node]
            
            for work in task:
                if self.queue.full():
                    self.set_node_status("busy", True)
               
                work.work_group = group_id
                self.queue.put(work)

            print "RCV task DONE"
        else:
            print "%s -> %s task id:%d %d" % (node['name'], self.name, task.id, task.get_task_count())
            # 未处理节点断线重添加情况
            
            if not self.__test_task(task.name):
                print "RCV ", self.name, " tasks_status ERROR!!! drop task!!!"
                return
            
            self.__add_ref(task, 1)
            task.node = node
            task.init_plugin_process(self.plugins)
            self.tasks[task.id] = task
    
    def handler_node_status(self, node, key, value):
        """ 节点状态即将发生变化
        """
        if key == "done_id":
            try:
                task, parent = self.__id_to_child_task(value)
                node['tasks'].remove(task.id)
                self.__add_ref(parent, -1)
                task.done = True
            except:
                print "WARN! done_id no task founed!!!"
        
        if key == "works" and value == None and node['works']:
            for work in node['works']:
                # 应该检查下id_task是否真实存在
                self.__add_ref(work.id_task, -1)
                self.__add_work_ref(work, -1)
                
    def __get_busy_task(self):
        task_list = []
        for task in self.tasks.values():
            if self.tasks_status[task.name] == "run":
                task_list.append((task.get_task_count(), task))
        
        return sorted(task_list)[-1][1]
    
    def handler_node_idle(self, node):
        """ 处理简单的任务分发
            1、一次分配一段ip
        """
        
        self.set_node_status('task_count', 
                self.get_node_task_count(), target = node)
        
        if len(self.pending_task) > 0:
            child_task = self.pending_task.pop()
            if isinstance(child_task, list):
                assert node['works'] == None
                node['works'] = child_task
                self.set_node_task(node, child_task)
                return
        else:
            # 随机从任务队列中挑选一个执行任务分配
            try:
                task = self.__get_busy_task()
                child_task = task.split(1)
                self.__add_ref(task, 1)
            except:
                self.__try_push_work()
                return
        
        node['tasks'].add(child_task.id)
        self.set_node_task(node, child_task)
        
    def __try_request_task(self):
        sort_nodes = []
        for node in self.nodes.values():
            if node['name']:
                sort_nodes.append((node['task_count'], node))
        
        if len(sort_nodes) < 1:
            return
        
        max_node = sorted(sort_nodes)[-1][1] #, key=lambda x:x[0])
        if max_node['task_count'] > self.get_node_task_count() \
            or len(sort_nodes) == 1:
            self.set_node_status('idle', True, target=max_node, force_refresh=True)
        
    def __try_push_work(self):
        """ 尝试主动推送队列任务
                                查询空闲子节点
        """
        for node in self.nodes.values():
            if not node['busy'] \
                and not node['works'] \
                and len(node['tasks']) == 0 \
                and node['name']:
                works = []
                while len(works) < self.queue.maxsize/2:
                    try:
                        # task_id, work
                        work = self.queue.get(timeout = 1)
                        if work.done_evt:
                            # done_evt 会 -1 ref
                            work.done_evt = None
                        else:
                            self.__add_ref(work.id_task, 1)
                        works.append(work)
                    except:
                        break
                    
                if len(works) > 0:
                    node['works'] = works
                    self.set_node_task(node, works)
                    self.set_node_status('task_count', self.get_node_task_count(), target = node)
                break
    
    def __works_done(self, task):
        """ 任务完成，向任务发起者报告
        """
        self.__add_ref(task, -1)
    
    def __worker_thread(self, thread_id):
        """ 任务分发线程
        """
        while True:
            try:
                work = self.queue.get(timeout = 1)
                
                try:
                    self.thread_idle.remove(thread_id)
                except: pass

                self.set_node_status("idle", False)
                
                if self.__test_task(work.task_name):
                    task_info = {'work':work.current, \
                                 'process':work.process, \
                                 'plugin':work.plugin, \
                                 'task':work.task_name}
                    
                    self.plugins[work.plugin].handle_task(task_info)
                
                self.__add_work_ref(work, -1)
                
                if work.done_evt:
                    func, argv = work.done_evt
                    func(argv)

            except Queue.Empty:
                self.thread_idle.add(thread_id)
                
                if len(self.thread_idle) == self.max_thread:
                    if self.status['idle'] and time.time() - self.idle_time < 5:
                        continue
                    
                    self.status['idle'] = True
                    self.idle_time = time.time()
                    self.__try_request_task()

    def __remove_task(self, task_name):
        print self.name, "!!!!remove task:", task_name
        self.tasks_status[task_name] = 'del'
        #清除队列tasks以及工作队列中的work
        for task in self.tasks.values():
            if task.name == task_name:
                task.flags |= self.FLAG_PENDING_DEL

    def __test_task(self, task_name):
        """ 测试指定的任务是否可用
        """
        if task_name not in self.tasks_status.keys():
            return False
        if self.tasks_status[task_name] == 'del':
            return False
        # run or pause
        return True
                
    def __run(self):
        self.__init_threads()
        while True:
            if self.status['idle']:                
                # test pending_task
                if len(self.pending_task) > 0:
                    self.handler_node_task(self.pending_task.pop())
                else:
                    time.sleep(0.1)
                       
            for key, task in self.tasks.items():
                if self.tasks_ref[id(task)] <= 0:
                    if task.node:
                        self.set_node_status("done_id", task.id, task.node, True)
                    else:
                        self.__remove_task(task.name)
                        self.send_msg("DEL_TASK", task.name)
                        
                    self.tasks.pop(key)
                    continue
                
                if task.flags & self.FLAG_PENDING_DEL:
                    self.tasks.pop(key)
                    self.tasks_ref.pop(id(task))
                    if task.name in self.tasks_status.keys():
                        # 等待当前队列中所有work完成
                        while not self.status['idle']:
                            time.sleep(0.1)
                        print self.name, "-task:", task.name, "del done!"
                        self.cfgs.pop(task.name)
                        self.tasks_status.pop(task.name)
                    continue
                
                if task.done or self.tasks_status[task.name] != "run":
                    continue

                try:
                    work = dispatch_work(task.name, task.move_next())
                    work.id_task = id(task)
                    if task.get_task_count() == 0:
                        work.done_evt = (self.__works_done, task)
                    
                    self.queue.put(work)
                except: pass
                
            # test node busy
            if self.queue.full():
                self.set_node_status("busy", True)
                self.__try_push_work()
            elif self.queue.qsize() < self.queue.maxsize/2:
                self.set_node_status("busy", False)
            
if __name__ == '__main__':
    server = engine()
    server.load_task("task.txt")
    server.load_task("task4.txt")
#    server.load_task("task2.txt")
    #server.run()
    #threading.Thread(target=server.run).start()

#    node1 = engine("node1.ini")
#    node2 = engine("node2.ini")
#    node3 = engine("node3.ini")
#    
#    node1.load_task("task.txt")
#    time.sleep(1)
#    node1.set_task_status("task", "pause")
#    time.sleep(10)
#    node1.set_task_status("task", "del")
#    node1.set_task_status("task", "run")

    