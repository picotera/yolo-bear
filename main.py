import logging
import urllib
import json
import time
from Queue import PriorityQueue
import threading
from bs4 import BeautifulSoup
import traceback
import requests
import signal
import sys

import fetcher
import helper
import rabbitcoat

# Requests is a very loud module, so disable the logging
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("pika").setLevel(logging.WARNING)

NORMAL_URL = 'https://%s/search?%s'

HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36'}

GOOGLE_DOMAINS = ['www.google.com','www.google.ad','www.google.ae','www.google.com.af','www.google.com.ag','www.google.com.ai','www.google.am','www.google.co.ao','www.google.com.ar','www.google.as','www.google.at','www.google.com.au','www.google.az','www.google.ba','www.google.com.bd','www.google.be','www.google.bf','www.google.bg','www.google.com.bh','www.google.bi','www.google.bj','www.google.com.bn','www.google.com.bo','www.google.com.br','www.google.bs','www.google.co.bw','www.google.by','www.google.com.bz','www.google.ca','www.google.cd','www.google.cf','www.google.cg','www.google.ch','www.google.ci','www.google.co.ck','www.google.cl','www.google.cm','www.google.com.co','www.google.co.cr','www.google.com.cu','www.google.cv','www.google.com.cy','www.google.cz','www.google.de','www.google.dj','www.google.dk','www.google.dm','www.google.com.do','www.google.dz','www.google.com.ec','www.google.ee','www.google.com.eg','www.google.es','www.google.com.et','www.google.fi','www.google.com.fj','www.google.fm','www.google.fr','www.google.ga','www.google.ge','www.google.gg','www.google.com.gh','www.google.com.gi','www.google.gl','www.google.gm','www.google.gp','www.google.gr','www.google.com.gt','www.google.gy','www.google.com.hk','www.google.hn','www.google.hr','www.google.ht','www.google.hu','www.google.co.id','www.google.ie','www.google.co.il','www.google.im','www.google.co.in','www.google.iq','www.google.is','www.google.it','www.google.je','www.google.com.jm','www.google.jo','www.google.co.jp','www.google.co.ke','www.google.com.kh','www.google.ki','www.google.kg','www.google.co.kr','www.google.com.kw','www.google.kz','www.google.la','www.google.com.lb','www.google.li','www.google.lk','www.google.co.ls','www.google.lt','www.google.lu','www.google.lv','www.google.com.ly','www.google.co.ma','www.google.md','www.google.me','www.google.mg','www.google.mk','www.google.ml','www.google.mn','www.google.ms','www.google.com.mt','www.google.mu','www.google.mv','www.google.mw','www.google.com.mx','www.google.com.my','www.google.co.mz','www.google.com.na','www.google.com.nf','www.google.com.ng','www.google.com.ni','www.google.ne','www.google.nl','www.google.no','www.google.com.np','www.google.nr','www.google.nu','www.google.co.nz','www.google.com.om','www.google.com.pa','www.google.com.pe','www.google.com.ph','www.google.com.pk','www.google.pl','www.google.pn','www.google.com.pr','www.google.ps','www.google.pt','www.google.com.py','www.google.com.qa','www.google.ro','www.google.ru','www.google.rw','www.google.com.sa','www.google.com.sb','www.google.sc','www.google.se','www.google.com.sg','www.google.sh','www.google.si','www.google.sk','www.google.com.sl','www.google.sn','www.google.so','www.google.sm','www.google.st','www.google.com.sv','www.google.td','www.google.tg','www.google.co.th','www.google.com.tj','www.google.tk','www.google.tl','www.google.tm','www.google.tn','www.google.to','www.google.com.tr','www.google.tt','www.google.com.tw','www.google.co.tz','www.google.com.ua','www.google.co.ug','www.google.co.uk','www.google.com.uy','www.google.co.uz','www.google.com.vc','www.google.co.ve','www.google.vg','www.google.co.vi','www.google.com.vn','www.google.vu','www.google.ws','www.google.rs','www.google.co.za','www.google.co.zm','www.google.co.zw','www.google.cat','www.google.xxx']

# Constantly not working
BAD_DOMAINS = ['www.google.cn',]

#GOOGLE_DOMAINS = ['www.google.com.ar']

SUBNET = '10.0.0.'

#TODO: Delete this?
OUTPUT_DIR = 'output\\'
def saveOut(name, cont):
    open(OUTPUT_DIR + name, 'w').write(cont.encode('utf8'))    
def printOut(sf, *elems):
    
    params = []
    for elem in elems:
        str_elem = elem
        if type(str_elem) != unicode:
            str_elem = str(elem)
        if type(str_elem) == unicode:
            params.append(str_elem.encode('utf8'))
        else:
            params.append(str_elem)
            
    print sf %tuple(params)
    
class GoogleParser(object):
    
    source_format = 'google %s'
    
    def __init__(self, logger, fetcher):
        self.logger = logger
        self.fetcher = fetcher
    
    def __getUrl(self, g_result, link):
        # Some domains use it I guess
        if link.has_attr('data-href'):
            url = link['data-href']
        else:
            url = link['href']
        # link to another search, images or something
        if url.find('/search') == 0:                    
            return None
        
        return url
    
    def __getTitle(self, g_result, link):
        try:
            span = link.span
            if span != None:
                title = span.get_text()
            else:
                title = link.get_text()
        # These are elements that we don't need
        except (Exception, AttributeError) as e:
            self.logger.error("Can't find text: \n%s\n%s" %(g_result, link))
            return None
        
        return title
    
    def __getMatch(self, g_result, link):
        try:
            st = g_result.find(class_='st')
            inner_spans = st.find_all('span')
            if len(inner_spans) == 0:
                match = st.get_text()
            else:
                match = inner_spans[-1].get_text()
            match = match.replace(u'\xa0', u'').strip('.').strip()
        except (Exception, AttributeError) as e:
            self.logger.error("Can't find match: \n%s" %g_result)
            return None
            
        return match
    
    def __fetchData(self, search, url, match):
        data = self.fetcher.Fetch(search, url, match)
        # If fetching/matching failed, return None. The user will have to check it out himself.
        if data == None:
            return None
        #TODO: return pygres.SaveArticle(data)
        return 1
    
    def Parse(self, source, query, data):
        results = []
        soup = BeautifulSoup(data)
        
        for g_result in soup.find_all(class_='g'):
            #raw_input('Press any key...')
            #os.system('cls')
            try:
                link = g_result.a
                url = self.__getUrl(g_result, link)
                if url == None:
                    continue
                title = self.__getTitle(g_result, link)
                if title == None:
                    continue
                match = self.__getMatch(g_result, link)
                if match == None:
                    continue
                
                printOut('\n\nQuery: %s, Title: %s', query, title)
                id = self.__fetchData(query.search, url, match)
                result = {'source': self.source_format %source,
                          'query': str(query),
                          'url': url,
                          'title': title,
                          'match': match,
                          'id': id}
                          
                
                #print 'Results: %s' %result
                results.append(result)
                
            except Exception, e:
                #TODO: Remove this
                print 'Outer exception..'
                self.logger.exception(traceback.format_exc())
                raw_input()
        
        return results    

class Query(object):
    ''' A query to be executed'''
    
    def __init__(self, search, hot_word, handler):
        self.search = search
        self.hot_word = hot_word
        self.handler = handler # Where to send the results
        
    def SendResults(self, site, results):
        self.handler.AddResults(self.search, self.hot_word, site, results)
        
    def __str__(self):
        return '%s %s' %(self.search, self.hot_word)
    
class SearchHandler(object):
    ''' A handler for a search. Will save all the results of a given search '''
    def __init__(self, logger, filename, query_count, sender, corr_id):
        print 'start init'
        self.sender = sender
        self.logger = logger
        self.count = 0
        self.query_count = query_count
        self.filename = filename
        self.results = {}
        self.corr_id = corr_id
        
        self.lock = threading.Lock()
        
        print 'done init'
        
    def AddResults(self, search, hot_word, site, results):
        if not self.results.has_key(search):
            self.results[search] = {}
        self.results[search][hot_word] = {'site' : site,
                                          'results' : results}
        
        # The lock is needed to not send results twice
        with self.lock:

            self.count += 1
            
            #print results
            self.logger.info('Got search "%s %s" result from %s. %s of %s' %(search, hot_word, site, self.count, self.query_count))
            if self.count == self.query_count:
                self.SaveResults()
                self.SendResults()
    
    def SendResults(self):
                
        self.sender.Send(json.dumps(self.results), corr_id = corr_id)
    
    def SaveResults(self):
        f = open(self.filename, 'w')
        
        f.write(json.dumps(self.results, indent=4))

class NormalQuery(threading.Thread):
    
    def __init__(self, logger, config, queries, site, parser):
        threading.Thread.__init__(self)
    
        self.logger = logger
        self.config = config
        
        self.parser = parser
        self.queries = queries
        
        self.site = site
        self.s = requests.Session()
        #self.s.verify = False
        
        self.s.headers = HEADERS
        
        self.rsz = 8
        self.pages = self.config.result_size / self.rsz # how many pages to get
        
        self.last_query = 0        
    
    def run(self):
        '''
        In case of errors, the query will be returned to the queue, and the thread will be terminated / paused for a while
        '''
        last_query = 0
        
        while True:
            q_time, query = self.queries.get()
            print query
            try:
                results = []
                for page in range(0, self.pages):

                    args = {'q' : str(query),
                            }
                    self.logger.debug('Search: "%s" page# : %s'%(query, page))
                    encoded_query = urllib.urlencode(args)
                    
                    last_query = time.time()
                    search_results = self.s.get(NORMAL_URL %(self.site, encoded_query))#, headers = {'cookie': self.cookie})
                    if search_results.status_code != 200:
                        self.queries.put((q_time, query))
                        if search_results.status_code == 503:
                            self.logger.error('Bot detection %s, terminating' %(search_results.reason))
                            return
                        else:
                            self.logger.error('Unknown error %s: %s, terminating' %(search_results.status_code, search_results.reason))
                            return
                    
                    results.extend(self.parser.Parse(self.site, query, search_results.text))
                    #results.extend(('weehee',))
                    
                    # sleep the remaining time before the next query
                    sleep_time = last_query + self.config.sleep_time - time.time()
                    if sleep_time > 0:
                        time.sleep(sleep_time)
                    
                query.SendResults(self.site, results)
                self.queries.task_done()
            except Exception, e:
                self.logger.exception('Exception in thread %s' %self.name)
                self.queries.put((q_time, query))
                return
                
class GoogleSearcher(object):
    
    def __init__(self, logger, config='settings.ini', rabbit_config = 'settings.ini'):
        self.config = helper.GoogleConfig(config)
        self.logger = logger
        
        self.fetcher = fetcher.GoogleFetcher(self.logger)
        self.parser = GoogleParser(self.logger, self.fetcher)
        
        # Querying threads
        self.queries = PriorityQueue()
        
        thread_count = min(self.config.max_threads, len(GOOGLE_DOMAINS))
        
        self.threads = []
        for i in xrange(thread_count):
            thread = NormalQuery(self.logger, self.config, self.queries, GOOGLE_DOMAINS[i], self.parser)
            thread.name = 'Query %s' %GOOGLE_DOMAINS[i]
            thread.start()
            self.threads.append(thread)
            
        # Initialize rabbit objects to transfer information
        self.sender = rabbitcoat.RabbitSender(rabbit_config, self.config.out_queue)
    
        # We're still using a receiver and not the responder because it's asynchronous
        self.receiver = rabbitcoat.RabbitReceiver(rabbit_config, self.config.in_queue, self.__rabbitCallback)
        self.receiver.start()
    
    def Backup(self):
        '''
        This method should be called if the program is shutting down, return all the search requests to the queue
        '''
        backer = rabbitcoat.RabbitSender(self.config, self.config.in_queue)
        #TODO: the backup here..
    
    def __rabbitCallback(self, ch, method, properties, body):
        print 'data', ch, method, properties, body
        print "Receiever: Received message with:"
        if (not (body is None)):
            print "\t body: %r" % (body)
            self.Search(json.loads(body), properties.correlation_id)
        else:
            print "\t an empty body"
        ch.basic_ack(delivery_tag = method.delivery_tag)
    
    def Search(self, parameters, corr_id):
        '''
        @param corr_id: The correlation ID of the search.
        '''
        # Create a handler for this search
        handler = SearchHandler(self.logger, 'output/%s.txt' %parameters['name'], len(parameters) * len(self.config.hot_words), self.sender, corr_id=corr_id)
        for key in parameters:
            print key
            for hot_word in self.config.hot_words:
                query = Query(parameters[key].lower(), hot_word, handler)
                self.queries.put((time.time(), query))

def sig_handler(signum = None, frame = None):
    # Do something
    sys.exit(0)

# Set a handler for SIGTERM
signal.signal(signal.SIGTERM, sig_handler)
    
def main():
    searcher = GoogleSearcher(helper.getLogger(logging.INFO))
    
if __name__ == '__main__':
    main()