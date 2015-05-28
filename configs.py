from ConfigParser import SafeConfigParser

SECTION_NAME = 'GOOGLE'

class GoogleConfig(object):
    '''Read a user configuration file, store values in instance variables'''

    def __init__(self,f='settings.ini'):
        self.file = f
        self.parser = SafeConfigParser()
        self.parser.read(self.file)
        
        self.updateAll()        
        
    def updateAll(self):
        '''Update and store all user settings'''
        
        self.search_count = self.parser.getint(SECTION_NAME,'search_count')
        self.fetcher_count = self.parser.getint(SECTION_NAME,'fetcher_count')
        
        words_file = self.parser.get(SECTION_NAME,'hot_words_file')
        self.hot_words = open(words_file).read().split(',')
        
        blacklist_file = self.parser.get(SECTION_NAME, 'blacklist_file')
        self.blacklist = open(blacklist_file).read().split(',')

        self.result_size = self.parser.getint(SECTION_NAME, 'result_size')
        self.sleep_time = self.parser.getint(SECTION_NAME, 'sleep_time')
        
        self.in_queue = self.parser.get(SECTION_NAME,'in_queue')
        self.out_queue = self.parser.get(SECTION_NAME,'out_queue')