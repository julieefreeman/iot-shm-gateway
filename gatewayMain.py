__author__ = 'rasha'
import csv, numpy as np, datetime, uuid, threading, serial, time
from kafka.client import KafkaClient
from kafka.consumer import SimpleConsumer
from kafka.producer import SimpleProducer
from collections import deque
from xbee import ZigBee
import serial
import struct

fft_size = 128
sensor_mac_addresses = []
data_queue = deque([])
PORT = '/dev/ttyUSB0'
BAUD_RATE = 9600
TIME_SIZE = 4
TYPE_SIZE = 1

class XBeeReceiver(threading.Thread):
    daemon = True

    def parse(self, response):
        packet = dict()
        packet["sensor_id"] = (''.join('{:02x}-'.format(x) for x in response["source_addr_long"]))[:-1]
        data_length = len(response["rf_data"])
        time_start = 0
        fft_start = time_start + TIME_SIZE
        type_start = fft_start + int(fft_size/2)

        packet["fft"] = [x for x in response["rf_data"][fft_start : type_start]]
        readingTypeArr = response["rf_data"][type_start : data_length]
        packet["reading_type"] = int.from_bytes(readingTypeArr, byteorder='big', signed=False)
        timeArr = response["rf_data"][time_start : fft_start]
        packet["time"] = int.from_bytes(timeArr, byteorder='big', signed=False)
        print(packet)
        return packet


    def run(self):
        stop = False
        ser = serial.Serial(PORT, BAUD_RATE)
        xbee = ZigBee(ser)
        i = 0
        while(True):
            response = xbee.wait_read_frame()
            data_queue.extend(self.parse(response))
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
    threads = [
       # KafkaProducer(),
        #KafkaConsumer(), #for classifying feature
	XBeeReceiver()
    ]

    for t in threads:
        t.start()
    time.sleep(5)
    while(True):
        print

if __name__ == '__main__':
    main()

