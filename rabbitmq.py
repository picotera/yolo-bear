#!/usr/bin/env python
import pika, os, uuid, logging,sys, getopt
logging.basicConfig()
import time
import json

import threading

import helper
# Parse CLODUAMQP_URL (fallback to localhost)

##urltext = 'amqp://qddxpjau:SMJ4jbIv97tiSQg7YDIw8RLMCjyWoVXg@turtle.rmq.cloudamqp.com/qddxpjau'
##furl = os.environ.get('CLOUDAMQP_URL', urltext)#'amqp://qddxpjau:SMJ4jbIv97tiSQg7YDIw8RLMCjyWoVXg@turtle.rmq.cloudamqp.com/qddxpjau')
###http://activemq-domainname.rhcloud.com/demo/message/OPENSHIFT/DEMO?type=topic

"""
This is the way callbacks should be, so that they can modify the rabbit object.
"""
def print_callback(ch, method, properties, body):
    print 'data', ch, method, properties, body
    print "Receiever: Received message with:"
    #if (properties.corr_id is None)):
    #    print "\t correlation ID: %s" % properties.corr_id
    if (not (body is None)):
        print "\t body: %r" % (body)
    else:
        print "\t an empty body"
    ch.basic_ack(delivery_tag = method.delivery_tag)

class RabbitChannel(object):

    def __init__(self, config):
        self.config = config
        self.__LoadConfig()
        self.__InitInfrastructure()

    def __LoadConfig(self):

        self.server =       self.config.rabbit_server
        self.user =         self.config.rabbit_user
        self.password =     self.config.rabbit_password
        self.vhost =        self.config.rabbit_vhost
        self.timeout =      self.config.rabbit_timeout
        
    def __InitInfrastructure(self):
        ampq_url = "amqp://%s:%s@%s/%s" % (self.user,self.password,self.server,self.vhost)
        url = os.environ.get('CLOUDAMQP_URL',ampq_url)#urltext)#
    
        params = pika.URLParameters(url)
        params.socket_timeout = self.timeout
        self.connection = pika.BlockingConnection(params) # Connect to CloudAMQP
        self.channel = self.connection.channel()
        
class RabbitSender(RabbitChannel):

    def __init__(self, config, queue, reply_to=None):
        RabbitChannel.__init__(self, config)
        
        self.queue = queue
        self.channel.queue_declare(queue, durable=True)
        self.reply_to = reply_to       
        
    def Send(self, data, corr_id=str(uuid.uuid4()),reply_to_queue=None):
        
        message = json.dumps(data)
        
        #reply_to_queue = self.validate_in_queue(queue)

        #this may be not so good performance-wize since you constantly open and close the connection
        
        # send a message
        self.channel.basic_publish(exchange='', 
                              routing_key=self.queue, 
                              body=message,
                              properties=pika.BasicProperties(
                                  delivery_mode = 2, # make message persistent
                                  correlation_id = corr_id,
                                  reply_to = self.reply_to,
                              ))
        print "Sender: Produced message with:"
        print "\t correlation ID: %s" % corr_id
        print "\t body: %s" % message
        
class RabbitReceiver(RabbitChannel, threading.Thread):
    
    def __init__(self, config, queue, callback):
        RabbitChannel.__init__(self, config)
        threading.Thread.__init__(self, name='RabbitReceiver %s' %queue)
        
        self.queue = queue
        self.channel.queue_declare(queue,durable=True) # Declare a queue
        
        self.callback = callback
    
    def run(self):
        ''' Bind a callback to the queue '''
        print "Receiver: starting to consume messeges"
        
        # set up subscription on the queue
        self.channel.basic_qos(prefetch_count=1)
        
        self.channel.basic_consume(self.callback,
                              self.queue,
                              no_ack=False)
        
        self.channel.start_consuming()
        
class  RabbitWrapper(object):

    ####################
    #   Defaults
    ####################

    default_server  = "turtle.rmq.cloudamqp.com"
    default_user = "qddxpjau"
    default_password = "SMJ4jbIv97tiSQg7YDIw8RLMCjyWoVXg"
    default_vhost = default_user
    default_queue = "TEST"
    default_message = "Ding!"
    default_timeout = 5
    default_sender = False

    ####################
    #  Initializers
    ####################

    def __init__(self, server=None, user=None, password=None, vhost=None, timeout=None):
        
        self.set_defaults(server, user, password, vhost, timeout)
        self.init_infrastructure()

    def set_defaults(self,server=default_server, user=default_user, password=default_password,
                     vhost=default_vhost, timeout=default_timeout):
        if (server is None):   server =   self.default_server
        if (user is None):     user =     self.default_user
        if (password is None): password = self.default_password
        if (vhost is None):    vhost =    self.default_vhost
        if (timeout is None):  timeout =  self.default_timeout

        self.server = server
        self.user = user
        self.password = password
        self.vhost = vhost
        self.timeout = timeout

    def init_infrastructure(self):
        ampq_url = "amqp://%s:%s@%s/%s" % (self.user,self.password,self.server,self.vhost)
        url = os.environ.get('CLOUDAMQP_URL',ampq_url)#urltext)#
    
        params = pika.URLParameters(url)
        params.socket_timeout = self.timeout
        self.connection = pika.BlockingConnection(params) # Connect to CloudAMQP

    ####################
    #   Validators
    ####################

    def validate_message(self, message):
        if (message is None):
            if (self.message is None):
                return self.default_message
            else:
                return self.message
        else:
            return message


    def validate_in_queue(self, queue):
        if (queue is None):
            if (self.in_queue is None):
                return self.default_queue
            else:
                return self.in_queue
        else:
            return queue


    def validate_out_queue(self, queue):
        if (queue is None):
            if (self.out_queue is None):
                return self.default_queue
            else:
                return self.out_queue
        else:
            return queue

    '''
    def validate_callback(self, callback):
        if (callback is None):
            return default_callback(self)
        else:
            return callback
    '''

    ####################
    #
    #   Simple APIs
    #
    ####################

    def SendChannel(self, queue):
        channel = self.connection.channel()
    
        channel.queue_declare(queue, durable=True) # Declare a queue
        
        return SendChannel(channel)
        
    def Receive(self, queue, callback):
        ''' Bind a callback to the queue '''
        queue = self.validate_in_queue(queue)
        # Create the callback function (callback returns a func)
        callback = callback(self)
        
        channel = self.connection.channel()

        #this may be not so good performance-wise since you constantly open and close the connection
        channel.queue_declare(queue,durable=True) # Declare a queue
        self.response = None
        print "Receiver: starting to consume messeges"
        # set up subscription on the queue
        channel.basic_qos(prefetch_count=1)
        
        channel.basic_consume(callback,
                              queue,
                              no_ack=False)
        
        channel.start_consuming()
        '''
        while True:
            time.sleep(10)
            self.connection.process_data_events()
        for method, properies, body in channel.consume('koby'):
            channel.basic_ack(method.delivery_tag) # Ackknowledge the message
            print body
        '''
            
    ####################
    #
    # RPC Functionnality
    #
    ####################


    def default_response(self,body):
        return "response: got %s" % str(body)

    def setResponseFunction(self,function=None):
        if (function is None):
            function = self.default_response
        self.response_function = function

    def Respond(self, ch, method, properties, body):
        response = self.response_function(body)

        ch.basic_publish(exchange='',
                         routing_key=properties.reply_to,
                         body=str(response),
                         properties=pika.BasicProperties(
                             correlation_id = properties.correlation_id,
                         ))
        ch.basic_ack(delivery_tag = method.delivery_tag)        

    def startRespnseServer(self,response_function=None):
        self.setResponseFunction(response_function)
        Respond(callback=Respond)

    def ask(self, send_queue=None, reply_to_queue=None, message=None, callback=None, corr_id=str(uuid.uuid4())):
        self.Send(send_queue, message, corr_id, reply_to_queue)
        self.Receive(reply_to_queue, callback)

        
####################
#
# MeatSpace Adapter
#
####################


def usage():
    print 'wrapper.py [--sender/--receiver] [-s <server>] [-u <user>] [-p <password>] [-v <vhost>] [-q <queue>] [-m <message>] [-t <timeout>]'
    sys.exit()
	
def main(argv):
    
    try:
        opts, args = getopt.getopt(argv,"SRhs:u:p:v:q:Q:m:t:",
                                   ["receiver""sender","server=",
                                    "user=","password=","vohst=",
                                    "in_queue=","out_queue=",
                                    "message=", "timeout="])
    except getopt.GetoptError:
        usage()    

    server = user = password = vhost = timeout = in_queue = out_queue = message = callback = None
    for opt, arg in opts:
        if opt == '-h':
            usage()
        elif opt in ("-S", "--sender"):
            message  = RabbitWrapper.default_message
        elif opt in ("-R", "--receiver"):
            callback  = RabbitWrapper.default_callback
        elif opt in ("-s", "--server"):
            server = arg
        elif opt in ("-u", "--user"):
            user = arg
        elif opt in ("-p", "--password"):
            password = arg
        elif opt in ("-v", "--vhost"):
            vhost = arg
        elif opt in ("-q", "--in_queue"):
            in_queue = arg
        elif opt in ("-Q", "--out_queue"):
            out_queue = arg
        elif opt in ("-m", "--message"):
            message = arg
        elif opt in ("-t", "--timeout"):
            timeout = arg
        else:
            usage()    

    #rabbit = RabbitWrapper(server, user, password, vhost, timeout)
    
    #rabbit.run()
    
    config = helper.Config()
    
    if in_queue != None:
        receiver = RabbitReceiver(config, in_queue, print_callback)
        receiver.run()
    elif out_queue != None: # and message != None:
        sender = RabbitSender(config, out_queue)
        sender.Send({'name' : message})
        
    
if __name__ == "__main__":
    main(sys.argv[1:])
