import logging
import urllib
import time
from Queue import PriorityQueue
import threading

import traceback
import requests
import signal
import sys

import cProfile

import configs
import handlers
import parsers
import fetchers
from helper import *
import rabbitcoat
import pygres

# Requests is a very loud module, so disable the logging
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("pika").setLevel(logging.WARNING)

NORMAL_URL = 'https://%s/search?%s'

GOOGLE_DOMAINS = [
    'www.google.com',
    'www.google.ad',
    'www.google.ae',
    'www.google.com.af',
    'www.google.com.ag',
    'www.google.com.ai',
    'www.google.am',
    'www.google.co.ao',
    'www.google.com.ar',
    'www.google.as',
    'www.google.at',
    'www.google.com.au',
    'www.google.az',
    'www.google.ba',
    'www.google.com.bd',
    'www.google.be',
    'www.google.bf',
    'www.google.bg',
    'www.google.com.bh',
    'www.google.bi',
    'www.google.bj',
    'www.google.com.bn',
    'www.google.com.bo',
    'www.google.com.br',
    'www.google.bs',
    'www.google.co.bw',
    'www.google.by',
    'www.google.com.bz',
    'www.google.ca',
    'www.google.cd',
    'www.google.cf',
    'www.google.cg',
    'www.google.ch',
    'www.google.ci',
    'www.google.co.ck',
    'www.google.cl',
    'www.google.cm',
    'www.google.com.co',
    'www.google.co.cr',
    'www.google.com.cu',
    'www.google.cv',
    'www.google.com.cy',
    'www.google.cz',
    'www.google.de',
    'www.google.dj',
    'www.google.dk',
    'www.google.dm',
    'www.google.com.do',
    'www.google.dz',
    'www.google.com.ec',
    'www.google.ee',
    'www.google.com.eg',
    'www.google.es',
    'www.google.com.et',
    'www.google.fi',
    'www.google.com.fj',
    'www.google.fm',
    'www.google.fr',
    'www.google.ga',
    'www.google.ge',
    'www.google.gg',
    'www.google.com.gh',
    'www.google.com.gi',
    'www.google.gl',
    'www.google.gm',
    'www.google.gp',
    'www.google.gr',
    'www.google.com.gt',
    'www.google.gy',
    'www.google.com.hk',
    'www.google.hn',
    'www.google.hr',
    'www.google.ht',
    'www.google.hu',
    'www.google.co.id',
    'www.google.ie',
    'www.google.co.il',
    'www.google.im',
    'www.google.co.in',
    'www.google.iq',
    'www.google.is',
    'www.google.it',
    'www.google.je',
    'www.google.com.jm',
    'www.google.jo',
    'www.google.co.jp',
    'www.google.co.ke',
    'www.google.com.kh',
    'www.google.ki',
    'www.google.kg',
    'www.google.co.kr',
    'www.google.com.kw',
    'www.google.kz',
    'www.google.la',
    'www.google.com.lb',
    'www.google.li',
    'www.google.lk',
    'www.google.co.ls',
    'www.google.lt',
    'www.google.lu',
    'www.google.lv',
    'www.google.com.ly',
    'www.google.co.ma',
    'www.google.md',
    'www.google.me',
    'www.google.mg',
    'www.google.mk',
    'www.google.ml',
    'www.google.mn',
    'www.google.ms',
    'www.google.com.mt',
    'www.google.mu',
    'www.google.mv',
    'www.google.mw',
    'www.google.com.mx',
    'www.google.com.my',
    'www.google.co.mz',
    'www.google.com.na',
    'www.google.com.nf',
    'www.google.com.ng',
    'www.google.com.ni',
    'www.google.ne',
    'www.google.nl',
    'www.google.no',
    'www.google.com.np',
    'www.google.nr',
    'www.google.nu',
    'www.google.co.nz',
    'www.google.com.om',
    'www.google.com.pa',
    'www.google.com.pe',
    'www.google.com.ph',
    'www.google.com.pk',
    'www.google.pl',
    'www.google.pn',
    'www.google.com.pr',
    'www.google.ps',
    'www.google.pt',
    'www.google.com.py',
    'www.google.com.qa',
    'www.google.ro',
    'www.google.ru',
    'www.google.rw',
    'www.google.com.sa',
    'www.google.com.sb',
    'www.google.sc',
    'www.google.se',
    'www.google.com.sg',
    'www.google.sh',
    'www.google.si',
    'www.google.sk',
    'www.google.com.sl',
    'www.google.sn',
    'www.google.so',
    'www.google.sm',
    'www.google.st',
    'www.google.com.sv',
    'www.google.td',
    'www.google.tg',
    'www.google.co.th',
    'www.google.com.tj',
    'www.google.tk',
    'www.google.tl',
    'www.google.tm',
    'www.google.tn',
    'www.google.to',
    'www.google.com.tr',
    'www.google.tt',
    'www.google.com.tw',
    'www.google.co.tz',
    'www.google.com.ua',
    'www.google.co.ug',
    'www.google.co.uk',
    'www.google.com.uy',
    'www.google.co.uz',
    'www.google.com.vc',
    'www.google.co.ve',
    'www.google.vg',
    'www.google.co.vi',
    'www.google.com.vn',
    'www.google.vu',
    'www.google.ws',
    'www.google.rs',
    'www.google.co.za',
    'www.google.co.zm',
    'www.google.co.zw',
    'www.google.cat',
    'www.google.xxx'
]

# Constantly not working
BAD_DOMAINS = [
'www.google.cn',]
 
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

class Query(object):
    ''' A query to be executed'''
    
    def __init__(self, search, hot_word, handler):
        self.search = search
        self.hot_word = hot_word
        self.handler = handler # Where to send the results
        
    def SendResults(self, results):
        self.handler.AddResults(results)
        
    def __str__(self):
        return '%s %s' %(self.search, self.hot_word)

class NormalQuerier(threading.Thread):
    
    def __init__(self, logger, config, query_queue, site, parser):
        threading.Thread.__init__(self)
    
        self.logger = logger
        
        self.config = config
        
        self.parser = parser
        self.query_queue = query_queue
        
        self.site = site
        self.s = requests.Session()
        #self.s.verify = False
        
        self.s.headers = GENERIC_HEADERS
        
        self.rsz = 8
        self.pages = self.config.result_size / self.rsz # how many pages to get
        
        self.last_query = 0        
    
    def run(self):
        '''
        In case of errors, the query will be returned to the queue, and the thread will be terminated / paused for a while
        '''
        last_query = 0
        while True:
            q_time, query = self.query_queue.get()
            try:
                results = []
                for page in range(0, self.pages):

                    args = {'q' : str(query),
                            }
                    self.logger.debug('Searching "%s" page #%s'%(query, page))
                    encoded_query = urllib.urlencode(args)
                    
                    last_query = time.time()
                    search_results = self.s.get(NORMAL_URL %(self.site, encoded_query))#, headers = {'cookie': self.cookie})
                    if search_results.status_code != 200:
                        self.query_queue.put((q_time, query))
                        if search_results.status_code == 503:
                            self.logger.error('Bot detection %s, terminating' %(search_results.reason))
                            return
                        else:
                            self.logger.error('Unknown error %s: %s, terminating' %(search_results.status_code, search_results.reason))
                            return
                    
                    results.extend(self.parser.Parse(self.site, query, search_results.text))
                    
                    # sleep the remaining time before the next query
                    sleep_time = last_query + self.config.sleep_time - time.time()
                    if sleep_time > 0:
                        time.sleep(sleep_time)
                    
                query.SendResults(results)
            except Exception, e:
                self.logger.exception('Exception in thread %s' %self.name)
                self.query_queue.put((q_time, query))
                return
                
class GoogleSearcher(object):
    
    # Parameters used for the search
    relevant_params = (NAME_PARAM,)
        
    def __init__(self, config='conf/google.conf', rabbit_config='conf/rabbitcoat.conf', pygres_config='conf/pygres.conf'):
        self.config = configs.GoogleConfig(config)
        self.logger = getLogger('google')
        
        self.logger.info('Initializing google bot')

        self.parser = parsers.GoogleParser(self.logger)
        
        # Querying threads
        self.query_queue = PriorityQueue()
        
        searcher_count = min(self.config.search_count, len(GOOGLE_DOMAINS))
        self.searchers = []
        for i in xrange(searcher_count):
            thread = NormalQuerier(self.logger, self.config, self.query_queue, GOOGLE_DOMAINS[i], self.parser)
            thread.name = 'Query %s' %GOOGLE_DOMAINS[i]
            thread.start()
            self.searchers.append(thread)
        
        # Fetching threads
        
        self.fetcher_queue = PriorityQueue()
        self.fetchers = []
        for i in xrange(self.config.fetcher_count):
            thread = fetchers.BasicFetcher(self.logger, self.fetcher_queue, pygres_config)
            thread.name = 'Fetcher %s' %i
            thread.start()
            self.fetchers.append(thread)
            
        # Initialize rabbit objects to transfer information
        self.sender = rabbitcoat.RabbitSender(self.logger, rabbit_config, self.config.out_queue)
    
        # We're still using a receiver and not the responder because it's asynchronous
        self.receiver = rabbitcoat.RabbitReceiver(self.logger, rabbit_config, self.config.in_queue, self.__rabbitCallback)
        self.receiver.start()
    
    def Backup(self):
        '''
        This method should be called if the program is shutting down, return all the search requests to the queue
        '''
        backer = rabbitcoat.RabbitSender(self.config, self.config.in_queue)
        #TODO: the backup here..
    
    def __rabbitCallback(self, data, properties):
        try:
            self.Search(data, properties.correlation_id)
        except Exception:
            self.logger.exception('Exception while searching %s, %s' %(data, properties))
    
    def Search(self, parameters, corr_id):
        '''
        @param corr_id: The correlation ID of the search.
        '''
        used_params = []
        # Create a handler for this search
        for param in self.relevant_params:
            value = parameters.get(param)
            if not value:
                continue
            used_params.append(value)
        
        results_count = len(used_params) * len(self.config.hot_words)
        handler = handlers.SearchHandler(self.logger, parameters, self.config.blacklist, self.sender, self.fetcher_queue, corr_id, results_count)

        for value in used_params:
            for hot_word in self.config.hot_words:
                query = Query(value.lower(), hot_word, handler)
                self.query_queue.put((time.time(), query))
    
def main():
    searcher = GoogleSearcher()
    
if __name__ == '__main__':
    main()