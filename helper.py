import logging
import logging.handlers
import sys
from ConfigParser import SafeConfigParser

MAX_LOG_SIZE = 10 * 1024 * 1024 # 50MB
BACKUP_COUNT = 5

def getLogger(level, file_level = logging.DEBUG, name='pygoogle', max_size = MAX_LOG_SIZE, backup_count = BACKUP_COUNT):
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    file_handler = logging.handlers.RotatingFileHandler('%s.log' %name, mode='a', maxBytes=max_size, backupCount=backup_count, encoding=None, delay=0)
    file_handler.setLevel(file_level)
    
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

class GoogleConfig(object):
    '''Read a user configuration file, store values in instance variables'''

    def __init__(self,f='settings.ini'):
        self.file = f
        self.parser = SafeConfigParser()
        self.parser.read(self.file)
        
        self.updateAll()        
        
    def updateAll(self):
        '''Update and store all user settings'''
        
        
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
        
        self.in_queue = self.parser.get('RABBITMQ','in_queue')
        self.out_queue = self.parser.get('RABBITMQ','out_queue')
        