import urllib2, urllib, random
import re, Queue, threading
import time

from sgmllib import SGMLParser
from task import *

class URLLister(SGMLParser):
    def __init__(self): 
        self.urls = []
        SGMLParser.__init__(self)
        self.is_h3 = None
        
    def start_a(self, attrs):                     
        href = [v for k, v in attrs if k=='href'] 
        #print self.is_h3, href
        if href and self.is_h3:
            self.urls.extend(href)
            
    def start_h3(self, attrs):
        self.is_h3 = True
    
    def end_h3(self):
        self.is_h3 = False

    #results = extractSearchResults(html)
    
class google_crawler(base_task):
    def __init__(self, name, cfg, plugins):
        base_task.__init__(self, name, plugins)
        self.nodes = {}
        self.queue = Queue.Queue()
        self.url_count = 0
        self.mutex = threading.Lock()
        
        self.urls = set()
        self.search_end = False
        
        self.host = cfg.get_cfg_vaule("google")
        self.search = cfg.get_cfg_vaule("google_keys")            # search keys
        
        try:
            self.useragent = cfg.get_cfg_vaule("useragent")
        except:
            self.useragent = 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:23.0) Gecko/20130406 Firefox/23.0'
            
        try:
            self.sleep = int(cfg.get_cfg_vaule("sleep"))
        except:
            self.sleep = 2
            
        try:
            self.search_start = int(cfg.get_cfg_vaule("start"))
        except:
            self.search_start = 0
 
    def handler_next(self):        
        if len(self.urls) > 0:
            return self.urls.pop()
        
        queryStr = urllib2.quote(self.search)
        url = '%s/search?q=%s&start=%d' % (self.host, queryStr, self.search_start)
        
        request = urllib2.Request(url)
        request.add_header('User-agent', self.useragent)
        #request.add_header('Referer', '%s/?gws_rd=ssl' % self.host)

        time.sleep(random.randint(1, self.sleep))
        
        try:
            response = urllib2.urlopen(request, timeout=15)
            html = response.read()
        except Exception,e:
            print e
            self.search_end = True
            raise Exception('search end')
        
        url_lister = URLLister()
        url_lister.feed(html)
                
        if len(url_lister.urls) <= 1:
            # search end
            self.search_end = True
            raise Exception('search end')
        
        #one page 10 resules
        self.urls = set(url_lister.urls[-10:])
        
        print "\r%d\t%s" % (self.search_start, url_lister.urls[-1])
        
        #print len(self.urls), self.search_start, url_lister.urls[0]        
        self.search_start += 10 #len(self.urls)
        return self.urls.pop()
    
    def get_task_count(self):
        if self.search_end:
            return 0
        # one page 10 urls
        return 10
