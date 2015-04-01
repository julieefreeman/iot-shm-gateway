__author__ = 'rasha'
import csv, numpy as np, datetime, uuid, threading, serial, time
from kafka.client import KafkaClient
from kafka.consumer import SimpleConsumer
from kafka.producer import SimpleProducer
from collections import deque

fft_size = 64
sensor_mac_addresses = []
data_queue = deque([])
#from sensor:
#unix time
#sensor/xbee id
#freq, xmag, ymag, zmag
#...

#12354353426
#345254
#36, 23, 45, 67
#37, 32, 43, 32sensor_mac_addressessensor_mac_addresses

#read data from Xbee
class XReceiver(threading.Thread):
    daemon = True

    def run(self):
        stop = False
        startTime = time.time()
        currTime = startTime
        ser = serial.Serial('/dev/ttyUSB0', 9600)
        reading = [0, 0, 0, 0]
        i = 0
        while(not stop and i < 108/4):
            reading = [ser.read(), ser.read(), ser.read(), ser.read()]
            print(reading)
            i+=1
            #print(reading[:len(reading)-2] + "," + str(currTime) + "\n")
        ser.close()
        
        #while True:
            reading = ser.readline()
            data_queue.append(reading) 
        ser.close

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

    sensor_mac_addresses=[]
    #Create Frequency Vector
    #freq_array=np.array((1))
    #for i in range(2,int(fft_size/2)):
    #    freq_i=np.array((i*fs/fft_size))
    #    freq_array=np.vstack((freq_array,freq_i))
    with open('sensor_macs.csv', 'rt') as f:
        print('opening csv')
        reader=csv.reader(f)
        for row in reader:
            sensor_mac_addresses += row
    print(sensor_mac_addresses)

    threads = [
        KafkaProducer(),
        #KafkaConsumer(), #for classifying feature
	XBeeReceiver()
    ]

    for t in threads:
        t.start()
    time.sleep(5)

if __name__ == '__main__':
    main()

