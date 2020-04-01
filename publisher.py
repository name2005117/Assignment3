#!/usr/bin/python
# encoding: utf-8
import os              # OS level utilities
import sys
import argparse   # for command line parsing

import random
import time
import threading
import zmq
from kazoo.client import *
#from multiprocessing import Process


class Publisher:
    def __init__(self, zookeeper, port, topic):

        self.address = zookeeper
        server_address = zookeeper + ':2181'
        self.zk = KazooClient(hosts=server_address)
        if self.zk.state != KazooState.CONNECTED:
            self.zk.start()
        self.port = port
        self.topic = topic
        self.Connected = False
        self.Broker_IP = None
        self.socket = None
        # we randomly select the id for this publisher
        self.ID = str(random.randint(1, 10))
        self.path = []
        self.file = None
        self.list = []
        self.msgIndex = 0
        self.stime = None
        #self.Thread() = None
        
    
    def init(self):
        # self.zk = KazooClient(hosts=self.address + ':2181')
        while self.zk.state != KazooState.CONNECTED:
            print("current state is " + self.zk.state)
            self.zk.start()
    
        while self.zk.state != KazooState.CONNECTED:
            time.sleep(1)
            pass
        print('Pub %s connected to local ZooKeeper Server.' % self.ID)

        znode_path = '/Publishers/' + self.ID
        self.zk.create(path=znode_path, value=str(self.ID).encode('utf-8'), ephemeral=True, makepath=True)
                
        while self.zk.exists(znode_path) is None:
            pass
        
        # we find the file from path
        
        leader_path = '/Leader'
        data, state = self.zk.get(leader_path)
        self.file = './Output/' + self.ID + '-publisher.log'
        print(self.file)
        path1 = './Input/'+ self.topic[0] + '.txt'
        path2 = './Input/'+ self.topic[1] + '.txt'

        self.path.append(path1)
        self.path.append(path2)
        list1 = get_publications(path1)
        list2 = get_publications(path2)
        self.list.append(list1)
        self.list.append(list2)
        self.Broker_IP = data.decode("utf-8")
        
        if self.register_pub():
            print('Pub %s connected with leader' % self.ID)
            self.Connected = True

        print('PUB ID:', self.ID)


        @self.zk.DataWatch(path=leader_path)
        def watch_leader(data, state):
            print('***** watch leader *****')

            print("pub found leader change " + str(data) + " " + str(state))
            print('Broker in Leader Znode is: %s' % data)
            if state is None:
                self.Connected = False
                print('Pub %s loses connection with old leader' % self.ID)
            elif self.Connected is False:
                self.Broker_IP = data.decode("utf-8")
                # self.socket = None
                # print('pub %s try to reconnect with leader' % pub.ID)
                if self.register_pub():
                    print('pub %s connected with new leader' % self.ID)
                    #self.socket = None
                    self.Connected = True
                    time.sleep(2)
                    thr = self.get_pub_thread()
                    thr.start()

        

    def register_pub(self):

        print('Publisher NO. %s with %s.' % (self.ID, self.topic))

        # publisher to broker socket establish
        connection = "tcp://" + self.Broker_IP + ":5555"
        context = zmq.Context()
        self.socket = context.socket(zmq.REQ)
        # self.socket.setsockopt(zmq.LINGER, 5)
        self.socket.setsockopt(zmq.RCVTIMEO, 2000)
        self.socket.setsockopt(zmq.SNDTIMEO, 2000)

        current = time.time()
        stime = current # we record the time that publisher send 

        while (time.time() - current < 5):
            self.socket.connect(connection)

        if self.socket is None:
            print('Connection failed.')
            return False
        else:
            print('Connection succeed!')
            message = 'init' + '#' + self.ID + '#' + self.topic[0] +'#' + self.topic[1] +  '#' + str(stime)
            # send the message

            while True:
                try:
                    res = self.socket.send_string( message )
                    #print(res)
                    break
                except Exception as ex:
                    print("failed to register " + ex)
                time.sleep(1)

            while True:
                try:
                    recv_msg = self.socket.recv_string()
                    if recv_msg is not None:
                        break
                except Exception as ex:
                    print("failed to recv reg confirm " + str(ex))
                time.sleep(1)

            print(recv_msg)
            return True


    def get_pub_thread(self):
        def publishing():
            print("**** a new publishing thread ****")
            myBrokerIp = self.Broker_IP
            try:
                with open(self.file, 'a') as logfile:
                    i = 0
                    for t in self.list:

                        for p in t[self.msgIndex:]:
                            logfile.write('*************************************************\n')
                            #print(i)
                            logfile.write('Publish Info: %s  \n'% self.topic[i] )
                            logfile.write('Publish: %s\n' % p)
                            logfile.write('Time: %s\n' % str(time.time()))
                            sending = 'publish' + '#' + self.ID + '#' + self.topic[i] +'#' + p
                            # publish + ID + topic + p


                            while True:
                                try:
                                    self.socket.send_string(sending)
                                    break
                                except Exception as ex:

                                    print('failed to send msg ' + str(ex))
                                    try:
                                        curBroker = self.zk.get("Leader")[0].decode("utf-8")
                                        if curBroker != myBrokerIp:
                                            print("gracefully exit the thread")
                                            exit(0)
                                    except Exception as ex:
                                        print(ex)
                                time.sleep(1)

                            while True:
                                try:
                                    rcv_msg = self.socket.recv_string()
                                    print("recieved msg:" + rcv_msg)
                                    break
                                except Exception as ex:
                    
                                    try:
                                        print('failed to recv msg ' + str(ex))
                                        curBroker = self.zk.get("Leader")[0].decode("utf-8")
                                        if curBroker != myBrokerIp:
                                            print("gracefully exit the thread")
                                            exit(0)
                                    except Exception as ex:
                                        print(str(ex))
                                time.sleep(1)

                            self.msgIndex += 1
                            #i+=1


                            time.sleep(1)
                        i += 1
                        self.msgIndex =0
                    #print()
                    #self.socket.close()
            except KeyError:
                #print('Open or write file error.')
                end = 'end' + '#' + id + '#' 
                print(end)
                self.socket.send_string(end)
                self.socket.close()
            

        return threading.Thread(target=publishing, args=())
    

    def start(self):
        self.init()
        thr = self.get_pub_thread()
        thr.start()
        # thr.join()
    
    


def parseCmdLineArgs ():
    # parse the command line
    parser = argparse.ArgumentParser ()

    # add optional arguments
    parser.add_argument('-i', '--ip', type=str, help='current publisher ip address')
    parser.add_argument('-z', '--zk', type=str, help='ZooKeeper address')
    # parse the args
    args = parser.parse_args ()

    return args




	# registation finished
    #the_socket.close()

def get_publications(file_path):
	try:
		with open(file_path, 'r') as file:
			pubs = file.readlines()
		for i in range(len(pubs)):
			pubs[i] = pubs[i][:-1]
		return pubs
	except IOError:
		print('Open or write file error.')
		return []

def main():
    
    args = parseCmdLineArgs()
    
    zoo_address = args.zk

    # we define all the topics we have in this section
    topics = {1:'animals', 2:'countries', 3:'foods'}
    
    # select two topics randomly
    topic =[]
    topic.append(topics[2])
    topic.append(topics[1])
    #topic.append(topics[random.randint(1, 3)])

    #topic = topics[1]

    # we first init the publish server and connect with the zookeeper
    pub = Publisher(zoo_address,'5555',topic)

    # wait for the registation complete
    time.sleep(2)
    
    # we find the path for the topic
   
    pub.file = './Output/' + pub.ID + '-publisher.log'
    pub.start()
    #wait()




if __name__ == '__main__':

    main()
    
    
    
