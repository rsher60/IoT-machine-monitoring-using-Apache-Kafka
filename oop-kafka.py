from time import sleep
from urllib.request import urlopen as uReq
from kafka import KafkaProducer

import xml.etree.ElementTree as ET
import requests



class Kafka_Pipeline:

    def __init__(self,bootstrap_servers,topicName , url):

        self.bootstrap = [bootstrap_servers]
        self.topicName = topicName
        self.url = url
        self.my_message = []
        

    def kafka_conn(self):

        global producer
        producer = KafkaProducer(bootstrap_servers = self.bootstrap)
        print("connection of the Producer established with Kafka Server")

        
    def web_parsing(self):
        web = uReq(self.url)
        page = web.read()
        web.close()
        tree = ET.fromstring(page)
        global root
        root = tree.getchildren()
        root= root.pop(1)

        columns = ['Device']

        for child in root:
                i=0
                make=''
                m_id=[]
                adapter=[]
                for name in child.attrib.values(): 
                        if i%2 == 0:
                                m_id.append(name)
                                for char in name:
                                        if char != '0':
                                                make +=char
                                        else:
                                                break	
                        else:
                                adapter.append(name)
                                
                        i+=1
                m_id.append(name)
                #print('Adapter',adapter)
                #print('M_id',m_id)
                #print('Make',make)
        
    
    def MachineId(self,ids):
        id=[]
        for values in ids:
                id.append(values)
        if id[0] == 'Device':
                return id[1]

    

    '''
    for child in root:
        names=[]
        
        for child in child:
                i=0
                suffix =[]
                for values in child.attrib.values():
                        suffix.append(values)
                        if i<1:
                                for column in columns:
                                        if values == column:
                                                if values == 'Device':
                                                        id = MachineId(child.attrib.values())
                                                else:
                                                        break
                                        else:
                                                try:
                                                        if columns.index(values):
                                                                continue
                                                except:
                                                        columns.append(values)
                                                        break	
                        i+=1
                names.append(SetNames(suffix[0],suffix[1]))
                print(names)
'''
