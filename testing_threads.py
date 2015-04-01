__author__ = 'rasha'
import time, threading
from collections import deque

data_queue = deque([])
count = 0

#from sensor:
#unix time
#sensor/xbee id
#freq, xmag, ymag, zmag
#...

#initialize Kafka producer - send data to storm and to classifier node
class ZigbeeReceiver(threading.Thread):
    daemon = True

    def run(self):
        while True:
            global count
            data_queue.append(count)
            count += 1

class KafkaProducer(threading.Thread):
    daemon = True
   
    def run(self):
        while True:
             if len(data_queue)!=0:
                 print(data_queue.popleft())

def main():    
        
    threads = [
        KafkaProducer(),
        #KafkaConsumer(), #for classifying feature
	ZigbeeReceiver()
    ]

    for t in threads:
        t.start()
    time.sleep(5)

if __name__ == '__main__':
    main()


