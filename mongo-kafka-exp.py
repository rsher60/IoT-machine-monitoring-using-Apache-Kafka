from time import sleep

from urllib.request import urlopen as uReq
import xml.etree.ElementTree as ET

import requests
from kafka import KafkaProducer


bootstrap_servers = ['localhost:9092']
topicName = '2202019'

producer = KafkaProducer(key_serializer= str.encode,bootstrap_servers = bootstrap_servers)



                


#####################_______website parsing _________________________________###############################

#url = "http://152.1.58.169:5000/current"
url ="https://smstestbed.nist.gov/vds/current"
web = uReq(url)

page = web.read()
web.close()



my_message = []

tree = ET.fromstring(page)

root = tree.getchildren()
root= root.pop(1)


columns = ['Device']


# 1st Table including machines,make id and manufacturer
for child in root:
        i=0
        make=""
        m_id=""
        adapter=""
        for name in child.attrib.values(): 
                if i%2 == 0:
                        # machine id
                        m_id = name
                        for char in name:
                                if char != '0':
                                        make +=char
                                else:
                                        break	
                else:
                        adapter=name
                        
                i+=1
#

def MakeNewColumns(colNo, existing_col):
        print(colNo,existing_col)
        while existing_col <= colNo :
                print(existing_col)
                existing_col = existing_col + 1
                
                

def GetValueString(count):
        string = ""
        i=0
        t = "'%s'"
        while i<=count:
                string = string + t + ','
                i+=1
        
        string = string[:-1]
        return ("Values("+string+")")


        
                

def MachineId(ids):
        id=[]
        for values in ids:
                id.append(values)
        if id[0] == 'Device':
                return id[1]

def SetNames(components,comp_name):
        t_name=""
        #print(components,comp_name)
        if components== 'Linear' or components == 'Rotary':
                t_name=components+comp_name
        else:
                t_name=components
        return t_name

#for Knowing the Device Stream
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
        #ExportColumns(names,id)





def Cond_Stream(data,M_id,comp):
        global Type
        #print("Condition Stream")
        
        for child in data:
                asset = []
                data_list=[]
                for values in child.attrib.values():
                        data_list.append(values)
                        Type=child.tag.partition('}')
                        
                data_list.insert(0,M_id)
                data_list.insert(1,Type[2])
                data_list.append(child.text)
                

                for co in comp:
                        asset.append(co)
                if asset[0] =='Linear' or asset[0] == 'Rotary':
                        bb = asset[0]+asset[1]
                else :
                        bb = asset[0]
                data_list.insert(0,bb)


                if len(data_list) == 10:
                        data_list.pop(8)
                                                
                else:
                        if len(data_list) == 9:
                                data_list.pop(7)
                                data_list.insert(7,"")
                                

def MakeTables(components,comp_name):
        t_name=[]
        types_name=['Samples','Condition','Events']
        for a,b in zip(components,comp_name):
                if a == 'Linear':
                        t_name.append(a+b)
                else:
                        if a == 'Rotary':
                                t_name.append(a+b)
                        else:
                                t_name.append(a)
        print(t_name)

        

def Samp_Stream(data,M_id,comp):
        global Type
        global my_message
        
        for child in data:
                asset = []
                data_list=[]
                for values in child.attrib.values():
                        data_list.append(values)
                        Type=child.tag.partition('}')
                        
                data_list.insert(0,M_id)
                data_list.insert(1,Type[2])
                data_list.append(child.text)
                

                for co in comp:
                        asset.append(co)
                if asset[0] =='Linear' or asset[0] == 'Rotary':
                        bb = asset[0]+asset[1]
                else :
                        bb = asset[0]
                data_list.insert(0,bb)
        my_message = data_list	
        print(my_message)



                

def Event_Stream(data,M_id,comp):
        global Type
        #print("Event Stream")
        
        for child in data:
                asset = []
                data_list=[]
                for values in child.attrib.values():
                        data_list.append(values)#p
                        Type=child.tag.partition('}')
                        #asset.append(co)
                data_list.insert(0,M_id)
                data_list.insert(1,Type[2])
                data_list.append(child.text)
                

                for co in comp:
                        asset.append(co)
                if asset[0] =='Linear' or asset[0] == 'Rotary':
                        bb = asset[0]+asset[1]
                else :
                        bb = asset[0]
                data_list.insert(0,bb)
                

def ID(id):
        for child in id:
                mid=[]
                for values in child.attrib.values():
                        mid.append(values)
                if mid[0] == 'Device':
                        return(mid[1])

def ExtractData():
        global p
        for child in root:
                for xy in child:
                        att = xy.attrib.values()
                        #if isinstance(MachineId(child.attrib.values()),str):
                        p= ID(child)
                        for abc in xy:
                                if abc.tag.partition('}')[2] == "Condition":
                                        Cond_Stream(abc,p,att)
                                else:
                                        if abc.tag.partition('}')[2] == "Samples":
                                                Samp_Stream(abc,p,att)
                                        else:
                                                Event_Stream(abc,p,att)

# Data Extracting Script
for child in root:
        tables=[]
        name =[]
        for child in child:
                tables.append(list(child.attrib.values())[0])
                name.append(list(child.attrib.values())[1])
                #print(child.attrib)
        

ExtractData()
    ###################________________________________end of website parsing part___________________________###########################


    #kafka part beigns

    #ack = producer.send(topicName ,key= my_message[4] )

while True:
    
    for m in my_message:
            
            ack = producer.send(topicName , key = m , value =b'recieved')
            metadata = ack.get()
    print(metadata.topic)
    print(metadata.partition)







