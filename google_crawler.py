import urllib2,urllib,random
import re, Queue, threading

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
    def __init__(self, name, host, search, plugins):
        base_task.__init__(self, name, plugins)
        self.nodes = {}
        self.queue = Queue.Queue()
        self.url_count = 0
        self.mutex = threading.Lock()
        self.host = host
        self.search = search            # search keys
        self.search_start = 0
        self.urls = set()
        self.search_end = False
        
    def handler_next(self):        
        if len(self.urls) > 0:
            return self.urls.pop()
        
        queryStr = urllib2.quote(self.search)
        url = '%s/search?hl=en&q=%s&start=%d' % (self.host, queryStr, self.search_start)
        
        request = urllib2.Request(url)
        request.add_header('User-agent', 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:23.0) Gecko/20130406 Firefox/23.0')
        response = urllib2.urlopen(request)
        html = response.read()
        
        url_lister = URLLister()
        url_lister.feed(html)
                
        if len(url_lister.urls) <= 1:
            # search end
            self.search_end = True
            raise Exception('search end')
        
        #one page 10 resules
        self.urls = set(url_lister.urls[-10:])
        
        print len(self.urls), self.search_start, url_lister.urls[0]
        
        self.search_start += len(self.urls)
        return self.urls.pop()
    
    def get_task_count(self):
        if self.search_end:
            return 0
        # one page 10 urls
        return 10
