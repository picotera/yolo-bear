import logging
import urllib
import urllib2
import json
import time
import socket
from Queue import PriorityQueue
import threading
from bs4 import BeautifulSoup
import re
import traceback
import requests
import signal
import sys
import socket

import helper
import rabbitmq# Requests is a very loud module, so disable the logging

logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("pika").setLevel(logging.WARNING)

NORMAL_URL = 'https://%s/search?%s'

HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36'}

GOOGLE_DOMAINS = ['www.google.com','www.google.ad','www.google.ae','www.google.com.af','www.google.com.ag','www.google.com.ai','www.google.am','www.google.co.ao','www.google.com.ar','www.google.as','www.google.at','www.google.com.au','www.google.az','www.google.ba','www.google.com.bd','www.google.be','www.google.bf','www.google.bg','www.google.com.bh','www.google.bi','www.google.bj','www.google.com.bn','www.google.com.bo','www.google.com.br','www.google.bs','www.google.co.bw','www.google.by','www.google.com.bz','www.google.ca','www.google.cd','www.google.cf','www.google.cg','www.google.ch','www.google.ci','www.google.co.ck','www.google.cl','www.google.cm','www.google.com.co','www.google.co.cr','www.google.com.cu','www.google.cv','www.google.com.cy','www.google.cz','www.google.de','www.google.dj','www.google.dk','www.google.dm','www.google.com.do','www.google.dz','www.google.com.ec','www.google.ee','www.google.com.eg','www.google.es','www.google.com.et','www.google.fi','www.google.com.fj','www.google.fm','www.google.fr','www.google.ga','www.google.ge','www.google.gg','www.google.com.gh','www.google.com.gi','www.google.gl','www.google.gm','www.google.gp','www.google.gr','www.google.com.gt','www.google.gy','www.google.com.hk','www.google.hn','www.google.hr','www.google.ht','www.google.hu','www.google.co.id','www.google.ie','www.google.co.il','www.google.im','www.google.co.in','www.google.iq','www.google.is','www.google.it','www.google.je','www.google.com.jm','www.google.jo','www.google.co.jp','www.google.co.ke','www.google.com.kh','www.google.ki','www.google.kg','www.google.co.kr','www.google.com.kw','www.google.kz','www.google.la','www.google.com.lb','www.google.li','www.google.lk','www.google.co.ls','www.google.lt','www.google.lu','www.google.lv','www.google.com.ly','www.google.co.ma','www.google.md','www.google.me','www.google.mg','www.google.mk','www.google.ml','www.google.mn','www.google.ms','www.google.com.mt','www.google.mu','www.google.mv','www.google.mw','www.google.com.mx','www.google.com.my','www.google.co.mz','www.google.com.na','www.google.com.nf','www.google.com.ng','www.google.com.ni','www.google.ne','www.google.nl','www.google.no','www.google.com.np','www.google.nr','www.google.nu','www.google.co.nz','www.google.com.om','www.google.com.pa','www.google.com.pe','www.google.com.ph','www.google.com.pk','www.google.pl','www.google.pn','www.google.com.pr','www.google.ps','www.google.pt','www.google.com.py','www.google.com.qa','www.google.ro','www.google.ru','www.google.rw','www.google.com.sa','www.google.com.sb','www.google.sc','www.google.se','www.google.com.sg','www.google.sh','www.google.si','www.google.sk','www.google.com.sl','www.google.sn','www.google.so','www.google.sm','www.google.st','www.google.com.sv','www.google.td','www.google.tg','www.google.co.th','www.google.com.tj','www.google.tk','www.google.tl','www.google.tm','www.google.tn','www.google.to','www.google.com.tr','www.google.tt','www.google.com.tw','www.google.co.tz','www.google.com.ua','www.google.co.ug','www.google.co.uk','www.google.com.uy','www.google.co.uz','www.google.com.vc','www.google.co.ve','www.google.vg','www.google.co.vi','www.google.com.vn','www.google.vu','www.google.ws','www.google.rs','www.google.co.za','www.google.co.zm','www.google.co.zw','www.google.cat','www.google.xxx']

# Constantly not working
BAD_DOMAINS = ['www.google.cn',]

#GOOGLE_DOMAINS = ['www.google.com.ar']

SUBNET = '10.0.0.'

def FetchData(logger, url): #, g_match):
    ''' Fetch a given url and try to find the google matches in it '''     
    #patt = re.compile('\.+ ')
    #matches = patt.split(g_match)
            
    try:
        data = urllib2.urlopen(url, timeout=2).read()
    except urllib2.HTTPError, e:
        # This can happen due to not being authorized, or the site being down
        logger.error("Couldn't fetch page: %s" %url)
        return ''
    except urllib2.URLError, e:
        logger.error("URLError: %s" %url)
        return ''
    except socket.timeout, e:
        print 'timed out %s' %url
        return ''
        
    '''   
    soup = BeautifulSoup(data)        
    results = []

    for match in matches:
        match = match.strip()
        if match == "":
            continue
        
        res = soup.find(text=re.compile(re.escape(match)))
        if res != None:
            final_res = res.parent.get_text().strip()
            results.append((match, final_res))
    '''
    
    return data

class GoogleParser(object):
    
    def __init__(self, logger):
        self.logger = logger
        
    def Parse(self, data):
        results = []
        soup = BeautifulSoup(data)
        
        for g_result in soup.find_all(class_='g'):
            #raw_input('Press any key...')
            #os.system('cls')            
            try:
                link = g_result.a
                # Some domains use it I guess
                if link.has_attr('data-href'):
                    url = g_result.a['data-href']
                else:
                    url = g_result.a['href']
                # link to another search, images or somethin
                if url.find('/search') == 0:                    
                    continue
                try:
                    span = link.span
                    if span != None:
                        title = span.get_text()
                    else:
                        title = link.get_text()
                # These are elements that we don't need
                except (Exception, AttributeError) as e:
                    self.logger.error("Can't find text: \n%s\n%s" %(g_result, link))
                    continue
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
                    continue
                
                result = {'url': url,
                          'title': title,
                          'match': match,}
                #print 'Results: %s' %result
                results.append(result)
                
            except Exception, e:
                print 'OUter exception..'
                self.logger.exception(traceback.format_exc())
        
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
    def __init__(self, logger, filename, query_count, sender):
        print 'start init'
        self.sender = sender
        self.logger = logger
        self.count = 0
        self.query_count = query_count
        self.filename = filename
        self.results = {}
        
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
                
        self.sender.Send(json.dumps(self.results))
    
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
                    
                    results.extend(self.parser.Parse(search_results.text))
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
    
    def __init__(self, logger):
        self.config = helper.Config()
        self.logger = logger
        
        self.parser = GoogleParser(self.logger)
        
        # Querying threads
        self.queries = PriorityQueue()
        
        #self.normal_thread = threading.Thread(target=self.NormalQuery)
        
        #self.ajax_thread = AjaxQuery(self.logger, self.config, self.queries)
        #self.ajax_thread.start()
        
        thread_count = min(self.config.max_threads, len(GOOGLE_DOMAINS))
        
        self.threads = []
        for i in xrange(thread_count):
            thread = NormalQuery(self.logger, self.config, self.queries, GOOGLE_DOMAINS[i], self.parser)
            thread.name = 'Query %s' %GOOGLE_DOMAINS[i]
            thread.start()
            self.threads.append(thread)
            
        # Initialize rabbit objects to transfer information
        self.sender = rabbitmq.RabbitSender(self.config, self.config.out_queue)
        
        self.receiver = rabbitmq.RabbitReceiver(self.config, self.config.in_queue, self.RabbitCallback)
        self.receiver.start()
        
        '''
        self.normal_thread = NormalQuery(self.logger, self.config, self.queries, 'www.google.com', self.parser)
        self.normal_thread.start()
        
        self.normal_thread2 = NormalQuery(self.logger, self.config, self.queries, 'www.google.ca', self.parser)
        self.normal_thread2.start()
        '''
    
    def Backup(self):
        '''
        This method should be called if the program is shutting down, return all the search requests to the queue
        '''
        backer = rabbitmq.RabbitSender(self.config, self.config.in_queue)
        # Do the backup here..
        
    def RabbitCallback(self, ch, method, properties, body):
        print 'data', ch, method, properties, body
        print "Receiever: Received message with:"
        #if (properties.corr_id is None)):
        #    print "\t correlation ID: %s" % properties.corr_id
        if (not (body is None)):
            print "\t body: %r" % (body)
            self.Search(json.loads(body))
        else:
            print "\t an empty body"
        ch.basic_ack(delivery_tag = method.delivery_tag)
    
    def Search(self, parameters):
        # Create a handler for this search
        handler = SearchHandler(self.logger, 'output/%s.txt' %parameters['name'], len(parameters) * len(self.config.hot_words), self.sender)
        for key in parameters:
            print key
            for hot_word in self.config.hot_words:
                query = Query(parameters[key], hot_word, handler)
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