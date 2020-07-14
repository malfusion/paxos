from threading import Thread
from paxos import PaxosNode
from collections import deque
import random
import time

class Node(Thread):
    def __init__(self, id):
        super().__init__();
        self.id = id
        self.paxosNode = None
        self.comm = None
        self.inbox = deque()
        self.active = True


    def initPaxos(self, nodes, comm, paxosConfig):
        self.comm = comm
        self.nodes = nodes
        self.paxosNode = PaxosNode(self.id, paxosConfig, comm)
        
    def getId(self):
        return self.id

    def run(self):
        print("I am thread", self.id)
        time.sleep(1)
        mesgs = ['zero', 'one', 'two', 'three', 'four', 'five', 'six', 'seven', 'eight', 'nine', 'ten']
        self.paxosNode.sync(mesgs[self.id])
        while(self.active):
            if len(self.inbox):
                self.processMesg(self.inbox.popleft())


    def onMesgDelivery(self, mesg):
        self.inbox.append(mesg)


    def processMesg(self, mesg):
        if mesg['protocol'] == 'paxos':
            self.paxosNode.processMesg(mesg)
        
        