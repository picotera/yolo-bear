import urllib2
import itertools
import re
import socket
import threading
from bs4 import BeautifulSoup
from Queue import PriorityQueue
import time
import requests

from helper import GENERIC_HEADERS

TIMEOUT = 2
# How many retries each link gets
MAX_RETRIES = 3

OUTPUT_DIR = 'output/'
def saveOut(name, cont):
    ''' A debugging function '''
    if type(cont) == unicode:
        open(OUTPUT_DIR + name, 'w').write(cont.encode('utf8'))    
    elif type(cont) == str:
        open(OUTPUT_DIR + name, 'w').write(cont)

def printOut(sf, *elems):
    ''' A debugging function '''
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

def isChild(tag, parent_name):
    
    # Check if an element has a parent
    for parent in tag.parents:
        if parent.name == parent_name:
            print 'ignoring'
            return True
            
    return False    

class FetchRequest(object):
    def __init__(self, queries, url, matches, callback):
        self.queries = queries
        self.url = url
        self.matches = matches
        self.callback = callback
        self.retries = 0
    
class BasicFetcher(threading.Thread):
    def __init__(self, logger, fetcher_queue):
        threading.Thread.__init__(self)
        
        self.logger = logger
        self.fetcher_queue = fetcher_queue
        
        # Sites are more likely to answer if we mimic a browser
        self.s = requests.Session()
        self.s.verify = False
        self.s.headers = GENERIC_HEADERS
    
    def AddRequest(self, request):
        if type(request) != FetchRequest:
            self.logger.error('Received unsupported request type %s' %type(request))
            return
        self.fetcher_queue.put((time.time(), request))
    
    def run(self):
        while True:
            r_time, request = self.fetcher_queue.get()
            try:
                data = self._fetch(request.queries, request.url, request.matches)
                request.callback(request, data)
            except Exception:
                request.retries += 1
                if request.retries < MAX_RETRIES:                    
                    self.fetcher_queue.put((r_time, request))   
                else:
                    self.logger.exception('Giving up on %s' %request.url)
                    request.callback(request, None)

    def __fetchUrl(self, url):
        # run() should catch the exceptions and handle them
        self.logger.debug('Fetching page: %s' %url)
        res = self.s.get(url, timeout=TIMEOUT)
        return res.text, res.encoding
        '''
        except urllib2.HTTPError, e:
            # This can happen due to not being authorized, or the site being down
            self.logger.error("Couldn't fetch page: %s" %url)
            return None
        except urllib2.URLError, e:
            self.logger.error("URLError: %s" %url)
            return None
        except socket.timeout, e:
            self.logger.error("Timed out: %s" %url)
            return None
        '''
            
    def _fetch(self, queries, url, matches):
        # Basic fetcher just gets the URL
        data, encoding = self.__fetchUrl(url)
        return data.encode(encoding)
    
class GoogleFetcher(BasicFetcher):

    # Elements to ignore when searching for the match
    ignored_elements = ('title', 'script')
    # Characters that create problems (Can be represented in different ways)
    # Some are in \\ because the get escaped by re.escape
    wildcard_characters = (("\\'", '.'), ('\\ ', '\\s+'),)
    
    # A percentage given to google matches from generic matches
    google_match_ratio = 0.5
    
    # Elements to ignore when rating parents
    ignored_parents = ('[document]', 'html', 'head', ) #'body'
    
    def __init__(self, logger, fetcher_count):
        BasicFetcher.__init__(self, logger)

    def _fetch(self, queries, url, matches):
        ''' 
        Fetch a given url and try to find the google matches in it 
        If the fetch wasn't successful / the matching inside the page wasn't, returns None
        '''
        data = self.__fetchUrl(url)
        
        return self.__findMatches(queries, data, matches)
    
    def __findMatch(self, soup, match):
    
        elems = []
        ignored = []
        
        match = re.escape(match)
        
        # Ignore matches less than min_length, too generic
        
        for char, wildcard in self.wildcard_characters:
            match = match.replace(char, wildcard)
            
        matches = soup.body.find_all(text=re.compile(match, re.I))
        
        for res in matches:
            parent = res.parent
            name = parent.name
            #DEBUG:print 'element name: %s' %name
            
            # Ignore ignored elements
            if name in self.ignored_elements:
                ignored.append(parent)
                #DEBUG:print 'Ignoring element %s' %name
                continue
            
            elems.append(parent)
            
            final_res = parent.text
            #print 'Appending elem: %s' %final_res.encode('utf8')[:200]
        
        return elems
    
    def __updateTags(self, soup, tags, matches, match_score):
        # The total score of all the matched tags
        total_score = 0
        for match in matches:
            
            matched_tags = self.__findMatch(soup, match)
            
            print 'Matches tags: %s, %s' %(matches, matched_tags)
            
            total_score += match_score * len(matched_tags)
            for tag in matched_tags:
                if not tags.has_key(tag):
                    tags[tag] = 0
                tags[tag] += match_score
                
        return total_score
    
    def __findMatches(self, queries, data, matches):
        soup = BeautifulSoup(data)
        tags = {} # Dictionary of Tag: score

        self.logger.debug('Finding matches in page: %s, %s' %(queries, matches))
        
        words = set()
        for query in queries:
            words.update(query.lower().split(' '))
        
        # Update the tags dictionary with generic matches, and count how many tags were found
        generic_score = self.__updateTags(soup, tags, words, 1)
        
        # Decide what score to give to more specific matches
        match_score = generic_score * self.google_match_ratio / len(matches)
        # Also increase minimum length to more than 
        total_score = self.__updateTags(soup, tags, matches, match_score) + match_score
                
        parent = self.__findCommonParent(tags)
        
        #TODO: Remove this, just return parent
        if parent == None:
            return None
        
        #saveOut('parent.html', parent.text)
        #raw_input('find matches ended')
        
        return parent
        
    def __findCommonParent(self, tags):
        ''' 
        Given a dictionary of tags and their scores, try to find the one that contains the most data and the least trash. 
        This is achieved by scoring all the parents of the element, and dividing the score by the data
        '''
        if len(tags) == 0:
            self.logger.debug('No tags to rate')
            return None
        elif len(tags) == 1:
            return tags.keys()[0]
        
        total_score = 0
        parents = {}
        
        now_loop = time.time()
        print 'Before loop 1 %s' %len(tags)
        # Save what the score of each elements is
        #TODO: Too slow, improve speed
        for tag in tags:
            tag_score = tags[tag]
            total_score += tag_score
            for a in tag.parents:
                if a.name in self.ignored_parents:
                    break
                # Save the tags of each parent
                if not parents.has_key(a):
                    parents[a] = set()
                parents[a].add(tag)
        print 'loop 1: %s' %(time.time() - now_loop)
        now_loop = time.time()
        
        best_parent = None
        max_score = 0
        minimum_ratio = 0.5 #TODO: Need to check the numbers
        
        for parent in parents:

            parent_tags = parents[parent]

            match_score = 0
            for tag in parent_tags:
                match_score += tags[tag]
            
            match_ratio = float(match_score) / total_score
            #print '\nMatch ratio: %s, %s, %s' %(match_ratio, match_score, total_score)
            if match_ratio > minimum_ratio:
                # Try to get the maximum match_score/content ratio            
                content_score = float(len(parent_tags)) / len(str(parent))
                
                if max_score < content_score:
                    best_parent = parent
                    max_score = content_score
        
        print 'loop 2: %s' %(time.time() - now_loop)
        #print 'Common: %s\n\n' %common
        #raw_input('Common parent finished\n')
        return best_parent