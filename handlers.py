from helper import *
import threading
import time
from urlparse import urlparse

from fetchers import FetchRequest

# List of supported protocols when fetching urls
SUPPORTED_PROTOCOLS = ('http', 'https')

class SearchHandler(object):
    ''' A parent class for all search handlers '''
    def __init__(self, logger, parameters, blacklist, sender, fetcher_queue, corr_id, query_count):
        self.parameters = parameters
        self.blacklist = blacklist
        self.sender = sender
        self.fetcher_queue = fetcher_queue # A queue all the fetchers are listening on
        self.logger = logger
        self.count = 0
        self.query_count = query_count
        self.corr_id = corr_id
        
        self.fetched = 0
        self.fetch_count = 0
        
        self.query_lock = threading.Lock()
        self.fetch_lock = threading.Lock()
        
        self.results = {}

    def __addResult(self, result):
        url = result[URL_KEY]
        queries = result[QUERY_KEY]    
        matches = result[MATCHES_KEY]
        
        if not self.results.has_key(url):
            # Put the matches in a set, to avoid duplicates
            result[MATCHES_KEY] = set(matches)
            self.results[url] = result
        else:
            self.results[url][MATCHES_KEY].update(matches)
            self.results[url][QUERY_KEY].extend(queries)
        
    def AddResults(self, results):
        for result in results:
            self.__addResult(result)
        
        # The lock is needed to not send results twice
        with self.query_lock:
            self.count += 1
            if self.count == self.query_count:
                self.__fetchResults()
    
    def __onFetch(self, request, id):
        '''Called when the fetchers finishes fetching
        Save the data
        ''' 
        # Even when a mistake occurs, increate the counter so that the server gets results.
        result = self.results.get(request.url, None)
        
        '''
        if data == None:
            id = None
        elif not result:
            self.logger.error("Unknown url in handler %s" %request.url)
            id = None
        else:
            #id = 1
            #self.logger.debug('Saving article %s' %request.url)
            #id = self.db_articles.AddArticle(data, SearchEngines.GOOGLE)
            self.logger.debug('Saved article with id %s' %id)
        '''
        if not result:
            self.logger.error("Unknown url in handler %s" %request.url)
            id = None
        result[ID_KEY] = id
        with self.fetch_lock:
            self.fetched += 1
            self.logger.debug('Fetched %s pages of %s' %(self.fetched, self.fetch_count))
            if self.fetched == self.fetch_count:
                self.__fetchDone()
    
    def __fetchResults(self):
        '''
        After we have all the query results, fetch the data from the pages, and deliever to the next node
        '''
        self.logger.info('Started fetching results')
        # The final results
        results = []
    
        self.fetch_count = 0
        # Locking to count the number of expected results first
        with self.fetch_lock:
            for url in self.results.keys():
                print self.fetch_count
                result = self.results[url]
                
                parsed = urlparse(result[URL_KEY])
                
                valid = True
                # Check if the protocol is supported
                if parsed.scheme not in SUPPORTED_PROTOCOLS:
                    self.logger.debug('Protocl %s not supported' %parsed.scheme)
                    valid = False
                else: 
                    # Check if the site is blacklisted
                    for site in self.blacklist:
                        if parsed.netloc.find(site) != -1:
                            self.logger.debug('Site blacklisted: %s by rule %s' %(parsed.netloc, site))
                            valid = False
                            break
                if not valid:
                    self.results.pop(url)                   
                    continue
                
                self.fetch_count += 1
                
                # Add a fetch request to the fetcher queue
                request = FetchRequest(result[QUERY_KEY], url, result[MATCHES_KEY], callback=self.__onFetch)
                self.fetcher_queue.put((time.time(), request))
    
    def __fetchDone(self):
        self.logger.info('Finished fetching, sending results')

        results = []
        for url in self.results:
            result = self.results[url]
            
            result[QUERY_KEY] = ','.join(result[QUERY_KEY])
            result.pop(MATCHES_KEY)
            
            results.append(result)

        self.__sendResults(results)
    
    def __sendResults(self, results, error=None):
        '''Sends the results to the next gear.'''
        if error:
            results = {ERROR_KEY: error}
        else:
            results = {RESULTS_KEY: results, QUERY_KEY: self.parameters}
            
        self.sender.Send(results, corr_id = self.corr_id)
