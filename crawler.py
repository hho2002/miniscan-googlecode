# -*- coding: gb2312 -*-
import httplib, urlparse, urllib2, HTMLParser
import re, Queue, threading
import gzip, zlib
import cPickle as pickle
 
from StringIO import StringIO

class link_parser(HTMLParser.HTMLParser):
    prog = re.compile("([a-zA-Z]+):")
    
    def __init__(self):
        self.nodes = []
        HTMLParser.HTMLParser.__init__(self)
        
    def handle_starttag(self, tag, attrs):
        #print "Encountered the beginning of a %s tag" % tag
        if tag == "a":
            if len(attrs) > 0:
                for (variable, value)  in attrs:
                    if self.prog.match(value):
                        continue
                    if variable == "href":
                        self.nodes.append(value)
                        
class web_crawler:
    def __init__(self, host, plugins):
        self.nodes = {}
        self.queue = Queue.Queue()
        self.url_count = 0
        self.thread_pool = []
        self.mutex = threading.Lock()
        self.host = host.split("\n")
        self.current = None
        self.current_host = 0
        self.current_plugin = 0
        self.plugin = plugins
        self.queue.put('/')
    
    def html_decode(self, html):
        for encoding in ('utf-8', 'gbk', 'big5'):
            try:
                html = html.decode(encoding)
                return html
            except:
                pass
        return html
    
    def deflate(self, data):
        # zlib only provides the zlib compress format, not the deflate format;
        # so on top of all there's this workaround:
        try:
            return zlib.decompress(data, -zlib.MAX_WBITS)
        except zlib.error:
            return zlib.decompress(data)

    def add_url(self, url):
        if url in self.nodes:
            return

#        print "scanning:", \
#            threading.current_thread().name, \
#            self.url_count, \
#            len(self.nodes), \
#            url
#        
#        print urlparse.urlparse(url).path
        
        req = urllib2.Request(url)
        #req.add_header("Accept-Charset", "utf-8;")
        req.add_header("Accept-Encoding", "gzip, deflate")
        
        try:
            response = urllib2.urlopen(req)
            #encoding = response.headers.getparam('charset')
            if response.info().get('Content-Encoding') == 'gzip':
                buf = StringIO(response.read())
                f = gzip.GzipFile(fileobj=buf)
                html = f.read()
            elif response.info().get('Content-Encoding') == 'deflate':
                html = self.deflate(response.read())
            else:
                html = response.read()
            
            html = self.html_decode(html)
        except urllib2.HTTPError, http_err:
            print "http err:", http_err.code
            return
        
        self.url_count += 1
        html_parser = link_parser()
        
        try:
            #html_parser.reset()
            html_parser.feed(html)
        except Exception, e:
            print e, type(e)
            pass
        
        # insert child urls -> task queue
        for url in html_parser.nodes:
            if not url in self.nodes:
                self.nodes[url] = False
                self.queue.put(url)
                
    def worker_thread(self):
        while True:
            try:
                host = self.host[self.current_host]
                url = self.queue.get(timeout = 1)
                self.add_url(host + url)
            except:
                if self.get_task_count() == 0:
                    break
        
    def get_task_count(self):
        count = len(self.host) - self.current_host
        if self.queue.empty() and count == 0:
            return 0
        
        return count
    
    def move_next(self):
        plugin = self.plugin[self.current_plugin]
        if self.current_plugin == 0:
            if self.queue.empty():
                self.current_host += 1
                if self.current_host == len(self.host):
                    raise Exception("task empty")
                
                self.nodes = {}
                self.queue.put('/')
                
            host = self.host[self.current_host]
            url = self.queue.get(timeout = 1)
            self.current = (host, url)
            
            try:
                self.add_url(host + url)
            except:
                print "URL ERROR:", host + url
        
        if self.current_plugin < len(self.plugin)-1:
            self.current_plugin += 1
        else:
            self.current_plugin = 0
        
        return (self.current, plugin)
    
    def split(self, count):
        pass
    
        
#clawler = web_crawler("http://172.16.16.99/")
#while True:
#    print clawler.move_next()