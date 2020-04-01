#!/usr/bin/python
# encoding: utf-8
# 

import zmq
import time
import random
import argparse
import threading
import multiprocessing 
from broker_lib import broker_lib
from kazoo.client import KazooState
from kazoo.client import KazooClient

log_file = './Output/broker.log'

def parseCmdLineArgs ():
    # parse the command line
    parser = argparse.ArgumentParser ()

    # add optional arguments
    parser.add_argument("-i", "--ip", type=str, help='self ip address')
    #parser.add_argument("-d", "--id", type=str, help='self id')
    parser.add_argument('-z', '--zk', type=str, help='ZooKeeper address')

	# parse the args
    args = parser.parse_args()
    
    return args



def main():
    
    args = parseCmdLineArgs()
    
    ip = args.ip
    zk_address = args.zk
    #id = args.id
    id = str(random.randint(1, 10))
    print('ZooKeeper Address: ' + zk_address) 
    
    # deploy the port number for the coming request from pubs, subs and current leader
    xpub = '5555'
    xsub = '5556'
    xleader = '5557'

    # we init the broker library(my id, zk_address, my ip address,pub_port, sub_port)
     
    broker = broker_lib(id, zk_address,ip, xpub, xsub)
    
    # elect the leader under the path /Leader
    leader_path = '/Leader'

    if broker.zk.exists(leader_path):
        # If the leader znode already exists, set the flag=false, specify broker to be the follower
        broker.leader_flag = False
        broker.watch_mode()
        # this broker has to watch whether the current broker is down or not
        
        leader_address = str(broker.zk.get(leader_path)[0])
        leader_address = leader_address[2:]
        leader_address = leader_address[:-1]
        print(leader_address)
        broker.syncsocket = broker.pull_msg(leader_address, xleader)
        # sync messages from leader and update data storage
        sync_data(broker)
        
    else:
        # If the leader znode doesn't exist, set the flag=true, specify broker to be the leader

        broker.zk.create(leader_path, value=broker.IP.encode('utf-8'), ephemeral=True, makepath=True)
        while broker.zk.exists(path=leader_path) is None:
            pass
        broker.leader_flag = True
        # if this broker is elected to be the leader, then start broker
        
        # socket for leader to send sync request to followers
        #broker.pubsyncsocket = None
        broker.pubsyncsocket = broker.sourcepush(xleader)
        if broker.pubsyncsocket != None:
            print('leader: syncsocket ok')
        

    pubsocket = broker.pubsocket
    print("Bound to port 5555 and waiting for any publisher to contact\n")
    subsocket = broker.subsocket
    print("Bound to port 5556 and waiting for any subscriber to contact\n")
    
    
    # we first register all the pub and sub then give them ownership strength
    while True:
        try:
            msg1 = pubsocket.recv_string()
            print("recved msg: " + msg1)
            break
        except Exception as ex:
            print("fail to recv init msg " + str(ex))
            time.sleep(1)
    
    while broker.pubsyncsocket != None:
        
        try:
            broker.pubsyncsocket.send_string(msg1)
            #print('-----')
            break
        except Exception as ex:
            print("failed to send sync " + str(ex))
            if len(broker.zk.get_children('/Brokers/')) <= 1:
                broker.pubsyncsocket = None

    
    msg11 = myRead(msg1)
    
    print("read val " + str(msg11))
    dict_update(msg11,broker)
    pubsocket.send_string('PUB-Info has been received!')
    print("sent confirm msg")
    #broker.syncsocket.send_string(msg1.encode('utf-8'))
    
    #pubsocket.send_string("Please send me the publisher \n")

    msg2 = subsocket.recv_string()

    msg22 = myRead(msg2)
    
    while broker.pubsyncsocket != None:
        print('*******')
        try:
            broker.pubsyncsocket.send_string(msg2)
            break
        except Exception as ex:
            print("failed to send sync " + str(ex))
            continue
    
    sdict_update(msg22,broker.subdict, broker.subscriber)
    subsocket.send_string('SUB-BROKER has been connected')
    #broker.syncsocket.send_string(msg2.encode('utf-8'))
    pub_thread = threading.Thread(target=pub_service, args=(broker,))
    sub_thread = threading.Thread(target=sub_service, args=(broker,))
    
    pub_thread.start()
    sub_thread.start()

    pub_thread.join()
    sub_thread.join()
    

   
def pub_service(broker):
    while True:
        message = broker.pubsocket.recv_string()
        msg=myRead(message)
        #broker.syncsocket.send_string(msg.encode('utf-8'))
        if broker.pubsyncsocket is not None:
            while True:
                try:
                    broker.pubsyncsocket.send_string(message)
                    break
                except Exception as ex:
                    print("failed to send sync " + str(ex))
                    continue

        # add the coming info to the dict
        dict_update(msg, broker)
        # reply to pub that this process is done
        broker.pubsocket.send_string('Done!!!')

def sub_service(broker): 
    
    while True:
        message = broker.subsocket.recv_string()
        msg = myRead(message)
        sdict_update(msg, broker.subdict, broker.subscriber)
        topic = []
        topic.append(msg[2])
        topic.append(msg[3])
        number1 = int(msg[4])
        number2 = int(msg[5])
        # we get two topics and we deal with it seperately

        

        if msg[0] == 'ask':
            # we first find whether the broker has this 
                    
            pub_msg = find(topic, broker, number1, number2) 

            # msg = topic1 + content1 + number1 + topic1 + content2 + number2
            # Broker will only send one topic at one time

            broker.subsocket.send_string(pub_msg)
                
            broker.pubsyncsocket.send_string(message)
                
            with open(log_file, 'a') as logfile:
                logfile.write('Reply to SUB %s  with topic %s and topic %s \n' % (msg[1], msg[2], msg[3]))
                
        elif msg[0] == 'end':
                    
            time.sleep(1)
            m = 'END!' +'#' 
            broker.subsocket.send_string(m)
                
        elif msg[0] == 'reg':

            broker.subsocket.send_string('Connected')              

def sync_data(broker):
    
    # we sync data from leader and update its lib
    # we only record the info about the dict and sub
    while broker.leader_flag is False:
        print('\n************************************\n')
        try:
            msg = myRead(broker.syncsocket.recv_string())
            
            # we got msg from leader
        except:
            print('Time out')
            continue
            
        print(msg)
        #msg
        if msg[0]=='init':
            dict_update(msg, broker)
        elif msg[0]=='publish':
            dict_update(msg, broker)
        else:
            sdict_update(msg, broker.subdict, broker.subscriber)
        
        #print(broker.pubdict)
        print('\n************************************\n')
        print('received sync msg from leader')
        # update the pub_info
        


def myRead(msg):
    info = msg.split('#')
    return info

def find(topic, broker, number1, number2):

    # we pick the first one to PUB
    '''if topic in publisher:
        owner = publisher[topic]
        length = len(dict[owner[0]][topic] )
        i = random.randint(1, length)
        content = dict[owner[0]][topic][i-1]
    else:
        content = 'Nothing'''
    number = []
    number.append(number1)
    number.append(number2)
    content ='#'

    for t in topic:
        i = 0

        if t in broker.publisher:
            #print(t)
                      
            owner = broker.publisher[t]
            #print(owner)
            length = len(broker.pubhistory[owner][t])
            print('Length the sub ask is '+ str(number[i]) +', the length pub has is '+ str(length))
            l=0
            for n in broker.pubhistory:
                l = len(broker.pubhistory[n][t])+ l
                print('We total get: '+ str(l))
            if l < number[i]:
                content ='Nothing'
                break

            if length < number[i]:
          
                rest= number[i] - length 
                s = broker.pubdict.get(t)
                print(s)
                # we turn all the keys into a list and we search the list
                for pub in s:
                    if pub[0] is owner:
                        pass
                    else:
                        print(pub[0])
                        content = content + t + '#'+ broker.pubhistory[pub[0]][t][rest-1] + '#' +str(rest-1) + '#'
                        

                #content ='Nothing'
            else:
                content = content + t + '#'+ broker.pubhistory[owner][t][number[i]-1] + '#' + str(number[i]-1) + '#'
                i = i+1
        else:
            content = 'Nothing'
        
    
    

    # when we receive the request from subs, we look up the dict to find whether there exits the info
    
    print(content)
    return content
    

def dict_update(msg, broker):
    
    if msg[0] == 'init':
        pubID = msg[1]
        topic = []
        topic.append(msg[2])
        topic.append(msg[3])
        time = float(msg[4])
        #print(topic)
        broker.pubhistory.update({pubID: {}})
        for x in topic:
            # history[pubID][topic][list]
            broker.pubhistory[pubID].update( {x:[]})
            try:
                if broker.publisher.get(x) is None:
                    # pubhistory records what the pub will publisher
                    broker.pubdict.update({x:[]})
                    # publisher contains the ID of the pub
                    broker.publisher.update({x:pubID}) 
                    # pubdict contains the list of pubs who has topic x
                    broker.pubdict[x].append((pubID,time))
                    # topic: pubID:time
                else:
                    broker.pubdict[x].append((pubID,time))
                    broker.pubdict[x]=sorted(broker.pubdict[x], key=lambda k: k[1], reverse = False)

                    #broker.pubhistory.update({pubID: {x:[]}})
                    # we sort the whole list according to the value, aka the arriving time 
                    # we only edit the pubdict if this pub doesn't show up in the first place
   
                with open(log_file, 'a') as logfile:
                    logfile.write('PUB-Init msg: %s init with topic %s\n' % (pubID, x))
                    print('broker.pubdict')
                    print(broker.pubdict)
            except KeyError:
                pass

    elif msg[0] == 'publish':
        pubID = msg[1]
        
        topic=msg[2]
        
        publish = msg[3]
        # no matter message from which publishers, all of them will restore.
        
            
        try:
            broker.pubhistory[pubID][topic].append(publish)
            with open(log_file, 'a') as logfile:
                logfile.write('Pub No.%s published %s with topic %s\n' % (pubID, publish, topic))
                #print(broker.pubhistory)
        except KeyError:
            pass

    elif msg[0] == 'end':
        pubID = msg[1]
        broker.unable.append(pubID)
        #list1 = list (broker.publisher.keys()) [list (broker.publisher.values()).index (pubID)]
        # we first edit the pubdict, delete all that contain pubID
        for topic in broker.pubdict:

            for x in topic:
                if x[0] is pubID:
                    broker.pubdict[topic].remove(x)

                    # we delete the info we got in the 
        
        # we update the publisher
        for stopic in broker.publisher:
            if broker.publisher[stopic] is pubID:
                #
                new = broker.pubdict[stopic][0]
                broker.publisher.update({stopic:new})
                # we select the first one to be the new publisher
        
        for pub in broker.pubhistory:
            if pub.key() is pubID:
                broker.pubhistory.pop(pub)
        




                


        

def sdict_update(msg, dict, sub):
    
    if msg[0] == 'reg':
        
        subID = msg[1]
        if dict.get(subID) == None:
            dict.update({subID: []})
        #sub[topic].append(subID)
    elif msg[0] == 'ask':
        subID = msg[1]
        topic = []
        topic.append(msg[2])
        topic.append(msg[3])
        #topic = msg[2]
        dict[subID].append(topic)
        #sub[topic].append(subID)
                
    elif msg[0] == 'end':
        subID = msg[1]
        #sub[topic].remove(subID)
        dict.pop(subID)
    
    
    

if __name__=="__main__":
	
    main()


