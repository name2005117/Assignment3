#!/usr/bin/python
# encoding: utf-8

import zmq
import time
import random
import argparse
import threading
import multiprocessing 
from kazoo.client import KazooState
from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError


leader_path = '/Leader'
log_file = './Output/broker.log'
xleader = '5557'

#self, zk_server, my_address, xsub_port, xpub_port
class Broker:
    
    def __init__(self, id, zk_server, ip, xpub, xsub):

        self.ID = id
        self.IP = ip
        self.pubsocket = self.bindp(ip, xpub)
        self.subsocket = self.binds(ip, xsub)
        self.syncsocket = None
        self.pubsynsocket = None
        zk_server = zk_server + ':2181'
        self.zk = KazooClient(hosts=zk_server)
        self.leader_flag = False
        #self.ownership = None


        # the publisher and the info they store
        self.pubdict = {}
        self.publisher = {} 
        self.pubhistory = {}
        self.subdict = {}
        self.subscriber = {}
        self.unable = []
    
        self.init_zk()

        
        print('\n************************************\n')
        print('Init MyBroker succeed.')
        print('\n************************************\n')
        
        with open(log_file, 'w') as logfile:
            logfile.write('Init Broker succeed \n')

    
    def bindp(self,ip, port):
        # we use REQ and REP to manage between publisher and broker
        context = zmq.Context()
        p_socket = context.socket(zmq.REP)
        p_socket.setsockopt(zmq.RCVTIMEO, 3000)
        p_socket.setsockopt(zmq.SNDTIMEO, 3000)

        p_socket.bind('tcp://*:'+ port)
        with open(log_file, 'a') as logfile:
            logfile.write('5555! \n')
        return p_socket
    
    def binds(self, ip, port):
        # we use PUB and SUB to manage sub and broker
        context = zmq.Context()
        s_socket = context.socket(zmq.REP)
        s_socket.bind('tcp://*:' + port)
        with open(log_file, 'a') as logfile:
            logfile.write('5556! \n')
        
        return s_socket
    
    def init_zk(self):
        if self.zk.state != KazooState.CONNECTED:
            self.zk.start()
        while self.zk.state != KazooState.CONNECTED:
            pass 
        # wait until the zookeeper starts
        
        print('Broker %s connected to ZooKeeper server.' % self.ID)
        
        # Create the path /Brokers
        if self.zk.exists('/Brokers') is None:
            self.zk.create(path='/Brokers', value=b'', ephemeral=False, makepath=True)
        while self.zk.exists('/Brokers') is None:
            pass

        # create my node under the path /Brokers
        znode_path = '/Brokers/' + self.ID
        self.zk.create(path=znode_path, value=b'', ephemeral=True, makepath=True)
        while self.zk.exists(znode_path) is None:
            pass
        print('Broker %s created a znode in ZooKeeper server.' % self.ID)
        

    def watch_mode(self):
        print("In watch mode")
        election_path = '/Brokers/'
        leader_path = '/Leader'

        @self.zk.DataWatch(path=leader_path) 
        def watch_leader(data, state):

            if self.zk.exists(path=leader_path) is None:

                # time.sleep(random.randint(0, 3))
                election = self.zk.Election(election_path, self.ID)
                election.run(self.win_election)
    
    def win_election(self):
        print("Win election")
        leader_path = '/Leader'
        if self.zk.exists(path=leader_path) is None:
            try:
                self.zk.create(leader_path, value=self.IP.encode('utf-8'), ephemeral=True, makepath=True)
            except Exception as ex:
                print("shouldn't elect")

        while self.zk.exists(path=leader_path) is None:
            pass

        self.leader_flag = True
        #self.start_broker()
        #self.syncsocket = None
        self.pubsyncsocket = self.sourcepush('5557')
        if self.pubsyncsocket != None:
            print('Broker %s started sending msg' % self.ID)

    def pull_msg(self, address, port):
        context = zmq.Context()
        socket = context.socket(zmq.PULL)
        socket.setsockopt(zmq.RCVTIMEO, 30000)
        socket.connect('tcp://' + address + ':' + port)
        return socket

    # Leader push msg to followers
    def sourcepush(self, port):
        context = zmq.Context()
        socket = context.socket(zmq.PUSH)
        socket.bind("tcp://*" + ':' + port)
        socket.setsockopt(zmq.SNDTIMEO, 3000)
        socket.setsockopt(zmq.RCVTIMEO, 3000)
        return socket


    def find(self, topic, number1, number2):

        number = []
        number.append(number1)
        number.append(number2)
        content ='#'

        for t in topic:
            i = 0

            if t in self.publisher:
                #print(t)
                          
                owner = self.publisher[t]
                #print(owner)
                length = len(self.pubhistory[owner][t])
                print('Length the sub ask is '+ str(number[i]) +', the length pub has is '+ str(length))
                l=0
                for n in self.pubhistory:
                    l = len(self.pubhistory[n][t])+ l
                    print('We total get: '+ str(l))
                if l < number[i]:
                    content ='Nothing'
                    break

                if length < number[i]:
              
                    rest= number[i] - length 
                    s = self.pubdict.get(t)
                    print(s)
                    # we turn all the keys into a list and we search the list
                    for pub in s:
                        if pub[0] is owner:
                            pass
                        else:
                            print(pub[0])
                            content = content + t + '#'+ self.pubhistory[pub[0]][t][rest-1] + '#' +str(rest-1) + '#'
                            

                    #content ='Nothing'
                else:
                    content = content + t + '#'+ self.pubhistory[owner][t][number[i]-1] + '#' + str(number[i]-1) + '#'
                    i = i+1
            else:
                content = 'Nothing'

        print(content)
        return content


    def dict_update(self, msg):
    
        if msg[0] == 'init':
            pubID = msg[1]
            topic = []
            topic.append(msg[2])
            topic.append(msg[3])
            time = float(msg[4])
            #print(topic)
            self.pubhistory.update({pubID: {}})
            for x in topic:
                # history[pubID][topic][list]
                self.pubhistory[pubID].update( {x:[]})
                try:
                    if self.publisher.get(x) is None:
                        # pubhistory records what the pub will publisher
                        self.pubdict.update({x:[]})
                        # publisher contains the ID of the pub
                        self.publisher.update({x:pubID}) 
                        # pubdict contains the list of pubs who has topic x
                        self.pubdict[x].append((pubID,time))
                        # topic: pubID:time
                    else:
                        self.pubdict[x].append((pubID,time))
                        self.pubdict[x]=sorted(self.pubdict[x], key=lambda k: k[1], reverse = False)

                        #broker.pubhistory.update({pubID: {x:[]}})
                        # we sort the whole list according to the value, aka the arriving time 
                        # we only edit the pubdict if this pub doesn't show up in the first place
       
                    with open(log_file, 'a') as logfile:
                        logfile.write('PUB-Init msg: %s init with topic %s\n' % (pubID, x))
                        print('broker.pubdict')
                        print(self.pubdict)
                except KeyError:
                    pass

        elif msg[0] == 'publish':
            pubID = msg[1]
            
            topic=msg[2]
            
            publish = msg[3]
            # no matter message from which publishers, all of them will restore.
            
                
            try:
                self.pubhistory[pubID][topic].append(publish)
                with open(log_file, 'a') as logfile:
                    logfile.write('Pub No.%s published %s with topic %s\n' % (pubID, publish, topic))
                    #print(broker.pubhistory)
            except KeyError:
                pass

        elif msg[0] == 'end':
            pubID = msg[1]
            self.unable.append(pubID)
            #list1 = list (broker.publisher.keys()) [list (broker.publisher.values()).index (pubID)]
            # we first edit the pubdict, delete all that contain pubID
            for topic in self.pubdict:

                for x in topic:
                    if x[0] is pubID:
                        self.pubdict[topic].remove(x)

                        # we delete the info we got in the 
            
            # we update the publisher
            for stopic in self.publisher:
                if self.publisher[stopic] is pubID:
                    #
                    new = self.pubdict[stopic][0]
                    self.publisher.update({stopic:new})
                    # we select the first one to be the new publisher
            
            for pub in self.pubhistory:
                if pub.key() is pubID:
                    self.pubhistory.pop(pub)


    def sdict_update(self, msg):
    
        if msg[0] == 'reg':
            
            subID = msg[1]
            if self.subdict.get(subID) == None:
                self.subdict.update({subID: []})
            #sub[topic].append(subID)
        elif msg[0] == 'ask':
            subID = msg[1]
            topic = msg[2:4]
            #topic = msg[2]
            self.subdict[subID].append(topic)
            #sub[topic].append(subID)
                    
        elif msg[0] == 'end':
            subID = msg[1]
            #sub[topic].remove(subID)
            dict.pop(subID)



    def sync_mode(self):
        # we sync data from leader and update its lib
        # we only record the info about the dict and sub
        while self.leader_flag is False:
            print('\n************************************\n')
            try:
                msg = self.syncsocket.recv_string().split("#")
                
                # we got msg from leader
            except Exception as ex:
                # print('Time out')
                print(ex)
                time.sleep(1)
                continue
                
            print(msg)
            #msg
            if msg[0]=='init':
                self.dict_update(msg)
            elif msg[0]=='publish':
                self.dict_update(msg)
            else:
                self.sdict_update(msg)
            
            #print(broker.pubdict)
            print('\n************************************\n')
            print('received sync msg from leader')
        # update the pub_info


    def start(self):
        
        # elect the leader under the path /Leader
        leader_path = '/Leader'
        '''
        if self.zk.exists(leader_path):
            # If the leader znode already exists, set the flag=false, specify broker to be the follower
            leader_flag = False
            self.watch_mode()
            # this broker has to watch whether the current broker is down or not
            
            leader_address = str(self.zk.get(leader_path)[0])
            leader_address = leader_address[2:]
            leader_address = leader_address[:-1]
            print(leader_address)
            syncsocket = self.pull_msg(leader_address, xleader)
            # sync messages from leader and update data storage
            self.sync_mode()
            
        else:
            # If the leader znode doesn't exist, set the flag=true, specify broker to be the leader

            self.zk.create(leader_path, value=self.IP.encode('utf-8'), ephemeral=True, makepath=True)
            while self.zk.exists(path=leader_path) is None:
                pass
            self.leader_flag = True
            # if this broker is elected to be the leader, then start broker
            
            # socket for leader to send sync request to followers
            #broker.pubsyncsocket = None
            self.pubsyncsocket = self.sourcepush(xleader)
            if self.pubsyncsocket != None:
                print('leader: syncsocket ok')
        '''

        try:
            # If the leader znode doesn't exist, set the flag=true, specify broker to be the leader

            self.zk.create(leader_path, value=self.IP.encode('utf-8'), ephemeral=True, makepath=True)
            while self.zk.exists(path=leader_path) is None:
                pass
            self.leader_flag = True
            # if this broker is elected to be the leader, then start broker
            
            # socket for leader to send sync request to followers
            #broker.pubsyncsocket = None
            self.pubsyncsocket = self.sourcepush(xleader)
            if self.pubsyncsocket != None:
                print('leader: syncsocket ok')

        except NodeExistsError:
            # If the leader znode already exists, set the flag=false, specify broker to be the follower
            self.leader_flag = False
            self.watch_mode()
            # this broker has to watch whether the current broker is down or not
            
            leader_address = str(self.zk.get(leader_path)[0])[2:-1]

            print(leader_address)
            self.syncsocket = self.pull_msg(leader_address, xleader)
            # sync messages from leader and update data storage
            self.sync_mode()


        pubsocket = self.pubsocket
        print("Bound to port 5555 and waiting for any publisher to contact\n")
        subsocket = self.subsocket
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
        
        while self.pubsyncsocket != None:
            
            try:
                self.pubsyncsocket.send_string(msg1)
                #print('-----')
                break
            except Exception as ex:
                print("failed to send sync " + str(ex))
                if len(self.zk.get_children('/Brokers/')) <= 1:
                    self.pubsyncsocket = None

        
        msg11 = msg1.split("#")

        print("read val " + str(msg11))
        self.dict_update(msg11)

        pubsocket.send_string('PUB-Info has been received!')
        print("sent confirm msg")
        #broker.syncsocket.send_string(msg1.encode('utf-8'))
        
        #pubsocket.send_string("Please send me the publisher \n")

        msg2 = subsocket.recv_string()

        msg22 = msg2.split("#")
        
        while self.pubsyncsocket != None:
            print('*******')
            try:
                self.pubsyncsocket.send_string(msg2)
                break
            except Exception as ex:
                print("failed to send sync " + str(ex))
                continue
        
        self.sdict_update(msg22)

        subsocket.send_string('SUB-BROKER has been connected')
    

        def pub_service():
            while True:
                message = self.pubsocket.recv_string()
                msg = message.split("#")
                #broker.syncsocket.send_string(msg.encode('utf-8'))
                if self.pubsyncsocket is not None:
                    while True:
                        try:
                            self.pubsyncsocket.send_string(message)
                            break
                        except Exception as ex:
                            print("failed to send sync " + str(ex))
                            continue
                #broker.syncsocket.send_string(msg2.encode('utf-8'))
                # add the coming info to the dict
                self.dict_update(msg22)
                # reply to pub that this process is done
                self.pubsocket.send_string('Done!!!')

        def sub_service():
            while True:
                message = self.subsocket.recv_string()
                msg = message.split("#")
                self.sdict_update(msg)
                topic = []
                topic.append(msg[2])
                topic.append(msg[3])
                number1 = int(msg[4])
                number2 = int(msg[5])
                # we get two topics and we deal with it seperately


                if msg[0] == 'ask':
                    # we first find whether the broker has this 
                            
                    pub_msg = self.find(topic, number1, number2) 

                    # msg = topic1 + content1 + number1 + topic1 + content2 + number2
                    # Broker will only send one topic at one time
                    print("pub msg: " + pub_msg)

                    self.subsocket.send_string(pub_msg)
                    
                    if self.pubsyncsocket is not None:
                        self.pubsyncsocket.send_string(message)
                        
                    with open(log_file, 'a') as logfile:
                        logfile.write('Reply to SUB %s  with topic %s and topic %s \n' % (msg[1], msg[2], msg[3]))
                        
                elif msg[0] == 'end':
                            
                    time.sleep(1)
                    m = 'END!' +'#' 
                    self.subsocket.send_string(m)
                        
                elif msg[0] == 'reg':

                    self.subsocket.send_string('Connected')  

        pub_thread = threading.Thread(target=pub_service, args=())
        sub_thread = threading.Thread(target=sub_service, args=())
            
        pub_thread.start()
        sub_thread.start()

        pub_thread.join()
        sub_thread.join()
    

'''
run instance
'''
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


    broker = Broker(id, zk_address,ip, xpub, xsub)
    broker.start()

main()