from kafka import KafkaConsumer

import sys


bootstrap_servers = ['localhost:9092']


topicname = '2202019'
consumer = KafkaConsumer(topicname,group_id = 'group220' , bootstrap_servers = bootstrap_servers,auto_offset_reset = 'earliest')




#TO LISTEN TO THE PRODUCER CONTINOUSLY :

while True:
    
    try:
        for message in consumer:
            print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,message.offset, message.key,message.value))
            #print(message.key)
            if message.key == b'UNAVAILABLE':
                print("THE MACHINE IS NOT SENDING ANY DATA")
 
    except KeyboardInterrupt:
        sys.exit()
