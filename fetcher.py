import urllib2
import itertools
import re
from bs4 import BeautifulSoup


#TODO: Delete this?
OUTPUT_DIR = 'output\\'
def saveOut(name, cont):
    if type(cont) == unicode:
        open(OUTPUT_DIR + name, 'w').write(cont.encode('utf8'))    
    elif type(cont) == str:
        open(OUTPUT_DIR + name, 'w').write(cont)


def isChild(tag, parent_name):
    
    # Check if an element has a parent
    for parent in tag.parents:
        if parent.name == parent_name:
            print 'ignoring'
            return True
            
    return False    

# Try to get the element that contains MOST of those elements
ignored_parents = ('[document]', 'html', 'head', ) #'body'
def findCommonParent(tags):
        '''
        Try to find the first common parent of the array
        '''        
        if len(tags) == 0:
            return None
        elif len(tags) == 1:
            return tags[0]
        
        total_score = 0
        parents = {}
        
        # Save what the score of each elements is
        for tag in tags:
            tag_score = tags[tag]
            total_score += tag_score
            for a in tag.parents:
                if a.name in ignored_parents:
                    continue
                # Save the tags of each parent
                if not parents.has_key(a):
                    parents[a] = set()
                parents[a].add(tag)
                
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
                #print 'Score: %s' %content_score
                if max_score < content_score:
                    #print 'Yoopi'
                    best_parent = parent
                    max_score = content_score
        
        #print 'Common: %s\n\n' %common
        #raw_input('Common parent finished\n')
        return best_parent

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
        
class GoogleFetcher(object):

    # Elements to ignore when searching for the match
    ignored_elements = ('title', 'script')
    # Characters that create problems (Can be represented in different ways)
    # Some are in \\ because the get escaped by re.escape
    wildcard_characters = (("\\'", '.'), ('\\ ', '\\s+'),)

    min_match_len = 3
    
    # A percentage given to google matches from generic matches
    google_match_ratio = 0.5
    
    def __init__(self, logger):
        self.logger = logger

    def Fetch(self, search, url, g_match): #, g_match):
        ''' 
        Fetch a given url and try to find the google matches in it 
        If the fetch wasn't successful / the matching inside the page wasn't, returns None
        '''              
        try:
            data = urllib2.urlopen(url, timeout=2).read()
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
            
        return self.__findMatches(search, data, g_match)
    
    def __findMatch(self, soup, match):
    
        elems = []
        ignored = []
        
        match = re.escape(match)
        
        # Ignore matches less than min_length, too generic
        
        for char, wildcard in self.wildcard_characters:
            match = match.replace(char, wildcard)
            
        printOut('New match: %s', match)
            
        matches = soup.find_all(text=re.compile(match, re.I))
        
        for res in matches:
            
            parent = res.parent
            name = parent.name
            #DEBUG:print 'element name: %s' %name
            
            # Ignore ignored elements
            if name in self.ignored_elements:
                ignored.append(parent)
                #DEBUG:print 'Ignoring element %s' %name
                continue
                
            # Ignore children of <head>
            if isChild(parent, 'head'):
                continue                
            
            elems.append(parent)
            
            final_res = parent.text
            #print 'Appending elem: %s' %final_res.encode('utf8')[:200]
        
        return elems
    
    def __updateTags(self, soup, tags, matches, match_score):
        # The total score of all the matched tags
        total_score = 0
        for match in matches:
            print '\nMatch: %s' %match.encode('utf8')
            
            matched_tags = self.__findMatch(soup, match)
            
            total_score += match_score * len(matched_tags)
            for tag in matched_tags:
                if not tags.has_key(tag):
                    tags[tag] = 0
                tags[tag] += match_score
                
        return total_score
    
    def __findMatches(self, search, data, g_match):
        soup = BeautifulSoup(data)
        tags = {} # Dictionary of Tag: score

        split_patt = re.compile('\.+ ')
        # Strip the matches and ignore ones that are too short
        matches = [x.strip() for x in split_patt.split(g_match) \
                   if len(x.split(' ')) >= self.min_match_len]
        
        words = search.lower().split(' ')
        '''
        # filter matches if needed
        for word in words:
            # Ignore all the matches with the search word in them
            filtered_matches = []
            for match in matches:
                if word not in match.lower():
                    filtered_matches.append(match)
            
            matches = filtered_matches
        '''
        print '\nGoogle match: %s' %g_match.encode('utf8')
        print 'Mathces: %s\n' %matches
        
        saveOut('fetch.html', data) 
        
        # Update the tags dictionary with generic matches, and count how many tags were found
        generic_score = self.__updateTags(soup, tags, words, 1)
        
        # Decide what score to give to more specific matches
        match_score = generic_score * self.google_match_ratio / len(matches)
        print 'Match score: %s, %s' %(match_score, generic_score)
        # Also increase minimum length to more than 
        total_score = self.__updateTags(soup, tags, matches, match_score) + match_score
                
        parent = findCommonParent(tags)
        
        #TODO: Remove this, just return parent
        if parent == None:
            return None
        
        saveOut('parent.html', parent.text)
        raw_input('find matches ended')
        
        return parent