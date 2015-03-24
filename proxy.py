import urllib
import urllib2
import socket
import threading

THREAD_COUNT = 100
PROXY_FILE = 'proxies.txt'
OUT_FILE = 'working_proxies2.txt'

def checkThread(proxy_iter, working_proxies, proxy_lock):

    while True:
        with proxy_lock:
            try:
                proxy = proxy_iter.next()
            except StopIteration:
                return
        try:
            proxy_handler = urllib2.ProxyHandler({'https': proxy})
            opener = urllib2.build_opener(proxy_handler)
            opener.addheaders = [('User-agent', 'Mozilla/5.0')]
            sock=opener.open('https://www.google.com/search?q=hello', timeout=2)
        except urllib2.HTTPError, e:
            #print 'Error code: ', e.code
            pass
        except Exception, detail:
            #print "ERROR:", detail
            pass
        else:
            print 'success %s' %proxy
            working_proxies.append(proxy)
            
    open('working_proxies.txt', 'w').write('working proxies')
      
def main():
    """
    proxy = urllib2.ProxyHandler({'http':'58.251.78.71:8088'})
    opener = urllib2.build_opener(proxy)
    urllib2.install_opener(opener)
    
    html = urllib2.urlopen('http://www.facebook.com/').read()"""
    proxy_list = set(open(PROXY_FILE).read().split('\n'))
    proxy_iter = iter(proxy_list)
    working_proxies = []
    proxy_lock = threading.Lock()
    
    threads = [threading.Thread(target=checkThread, args=(proxy_iter, working_proxies, proxy_lock))
               for i in xrange(THREAD_COUNT)]
               
    for thread in threads:
        thread.start()
        
    for thread in threads:
        thread.join()
    
    f = open(OUT_FILE, 'w')
    
    print working_proxies
    for proxy in working_proxies:
        f.write("%s\n" %proxy)

main()