import helper
import re
from bs4 import BeautifulSoup

SOURCE_FORMAT = 'google %s'

OUTPUT_DIR = 'output/'
def saveOut(name, cont):
    ''' A debugging function '''
    if type(cont) == unicode:
        open(OUTPUT_DIR + name, 'w').write(cont.encode('utf8'))    
    elif type(cont) == str:
        open(OUTPUT_DIR + name, 'w').write(cont)
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
    
    split_patt = re.compile('\.+ ')
    min_match_len = 3
    
    def __init__(self, logger):
        self.logger = logger
    
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
    
    def __getMatches(self, g_result, link):
        try:
            st = g_result.find(class_='st')
            if not st:
                return []
            inner_spans = st.find_all('span')
            if len(inner_spans) <= 1:
                if len(inner_spans) == 1:
                    text = inner_spans[0].get_text()
                    # This means it's the date which we ignore
                    if text[-3:] == ' - ':
                        inner_spans[0].extract()
                match = st.get_text()
            else:
                match = inner_spans[-1].get_text()
                
            match = match.replace(u'\xa0', u'').strip('.').strip()
            
        except (Exception, AttributeError) as e:
            self.logger.exception("Can't find match: \n%s" %g_result)
            return None
            
        # Strip the matches and ignore ones that are too short
        matches = [x.strip() for x in self.split_patt.split(match) \
                   if len(x.split(' ')) >= self.min_match_len]
        
        return matches
        
    def Parse(self, source, query, data):
        results = []
        soup = BeautifulSoup(data)
        
        for g_result in soup.find_all(class_='g'):
            try:
                link = g_result.a
                url = self.__getUrl(g_result, link)
                if not url:
                    continue
                title = self.__getTitle(g_result, link)
                if not title:
                    continue
                matches = self.__getMatches(g_result, link)
                if not matches:
                    continue
                
                '''
                if len(matches) == 0:
                    self.logger.debug('Zero matches: %s, %s, %s, %s' %(url, source, title, query))
                '''
                
                result = {helper.SOURCE_KEY: SOURCE_FORMAT %source,
                          helper.QUERY_KEY: [str(query)],
                          helper.URL_KEY: url,
                          helper.TITLE_KEY: title,
                          helper.MATCHES_KEY: matches,}
                          
                results.append(result)
                
            except Exception, e:
                self.logger.exception('Parse outer exception')
        
        return results    