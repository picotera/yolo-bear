import logging
import sys
from ConfigParser import SafeConfigParser

def getLogger(level, name='pygoogle'):
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    file_handler = logging.FileHandler('%s.log' %name)
    file_handler.setLevel(logging.DEBUG)
    
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger
        

class Config(object):
    '''Read a user configuration file, store values in instance variables'''

    def __init__(self,f='settings.ini'):
        self.file = f
        self.parser = SafeConfigParser()
        self.updateAll()
        
    def updateAll(self):
        '''Update and store all user settings'''
        self.parser.read(self.file)
        
        self.max_threads = self.parser.getint('MAIN','max_threads')
        
        '''
        proxy_file = self.parser.get('MAIN','proxy_file')
        self.proxies = open(proxy_file).read().split('\n')
        #self.proxies = ['115.29.246.204:80']
        '''
        
        words_file = self.parser.get('QUERY','hot_words_file')
        self.hot_words = open(words_file).read().split(',')
        
        self.result_size = self.parser.getint('QUERY', 'result_size')
        self.sleep_time = self.parser.getint('QUERY', 'sleep_time')