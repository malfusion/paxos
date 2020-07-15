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
        time.sleep(0.1)
        mesgs = ['zero', 'one', 'two', 'three', 'four', 'five', 'six', 'seven', 'eight', 'nine', 'ten']
        self.paxosNode.sync(mesgs[self.id])
        
        while not self.paxosNode.hasReachedConsensus():
            if len(self.inbox):
                self.processMesg(self.inbox.popleft())


    def onMesgDelivery(self, mesg):
        self.inbox.append(mesg)


    def processMesg(self, mesg):
        if mesg['protocol'] == 'paxos':
            self.paxosNode.processMesg(mesg)
        
        