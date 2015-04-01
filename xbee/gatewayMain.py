__author__ = 'rasha'
import csv, numpy as np, datetime, uuid, threading, serial, time
from kafka.client import KafkaClient
from kafka.consumer import SimpleConsumer
from kafka.producer import SimpleProducer
from collections import deque
import XBee
import binascii
fft_size = 64
data_queue = deque([])

class XBeeReceiver(threading.Thread):
    daemon = True
    def run(self):
        xbee = XBee.XBee("/dev/ttyUSB0")
        stop = False
        i = 0
        while(not stop and i < 108/4):
            packet = deque()
            msg = xbee.Receive()
            if(msg != None):
                print(binascii.hexlify(msg))
            else:
                print("None received")
            i+=1

#format data and send to Kafka
class KafkaProducer(threading.Thread):
    daemon = True

    def run(self):
        client = KafkaClient('ip-72-131-4-78-ec2.internal:6667')
        producer = SimpleProducer(client)        

        while True:
            if len(data_queue)!=0:
                data = data_queue.popleft()
                #format data here
                print(data)
                if True: # for when we send to storm topic
                    producer.send_messages('storm_topic',data)
                else: # for when we send to create the classifier
                    producer.send_messages('classifier_topic',data)

#check if building needs to be classified or analyzed
#class KafkaConsumer(threading.Thread):
#    daemon = True
#
#    def run(self):
#        client = KafkaClient('ip-72-131-4-78-ec2.internal:6667')
#        consumer = SimpleConsumer(client, 'building_status_topic')
#
#        for message in consumer:
#            print(message)

def main():    
    #Create Frequency Vector
    fft_size=1024
    fs=92
    freq_array=np.array((1*fs/fft_size))
    for i in range(2,int(fft_size/2)):
        freq_i=np.array((i*fs/fft_size))
        freq_array=np.vstack((freq_array,freq_i))
    #Create Frequency Vector
    #freq_array=np.array((1))
    #for i in range(2,int(fft_size/2)):
    #    freq_i=np.array((i*fs/fft_size))
    #    freq_array=np.vstack((freq_array,freq_i))

    threads = [
        #KafkaProducer(),
        #KafkaConsumer(), #for classifying feature
	XBeeReceiver()
    ]

    for t in threads:
        t.start()
    time.sleep(5)

if __name__ == '__main__':
    main()

