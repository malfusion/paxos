import threading
import time
import random

class Paxos:
    def __init__(self, paxosNodes):
        self.nodes = paxosNodes



class PaxosNode():
    def __init__(self, nodeId, paxosConfig, comm):
        self.paxosConfig = paxosConfig
        self.nodeId = nodeId
        self.comm = comm
        self.consensusValue = None

        #Proposer
        self.mesgVal = None
        self.ctr = 0
        self.permAccepts = {}
        self.proposalAccepts = {}

        #Acceptor
        self.lastPromised = None
        self.lastAccepted = None
        self.lastAcceptedValue = None
        
    def hasReachedConsensus(self):
        return self.consensusValue != None

    def processMesg(self, mesg):
        meta = mesg['metadata']
        data = mesg['data']
        if data['type'] == 'permission':
            self.rxPerm(meta['from'], meta['to'], data['suggestionId'])
        if data['type'] == 'permission_accept':
            self.rxPermAccept(meta['from'], meta['to'], data['suggestionId'])
        if data['type'] == 'permission_deny':
            self.rxPermDeny(meta['from'], meta['to'], data['suggestionId'], data['last_promised'], data['last_accepted'], data['last_accepted_value'])

        if data['type'] == 'propose':
            self.rxProposal(meta['from'], meta['to'], data['suggestionId'], data['value'])
        if data['type'] == 'proposal_accept':
            self.rxProposalAccept(meta['from'], meta['to'], data['suggestionId'])
        if data['type'] == 'proposal_deny':
            self.rxProposalDeny(meta['from'], meta['to'], data['suggestionId'], data['last_promised'], data['last_accepted'], data['last_accepted_value'])

        if data['type'] == 'accepted':
            self.rxAccepted(meta['from'], meta['to'], data['suggestionId'], data['value'])
        
        
    def PermissionMessage(self, suggId):
        return {
                'data': {
                    'type': 'permission',
                    'suggestionId': self.constructSuggId()
                },
                'protocol': 'paxos'
            }

            
    
    def constructSuggId(self):
        return '<' + str(self.ctr) + ',' + str(self.nodeId) + '>'

    def sync(self, value):
        self.mesgVal = value
        # TO REMOVE
        # if self.nodeId == 1:
        self.txPerm(self.nodeId)

    # Proposer Function
    def txPerm(self, fromNodeId):
        if self.ctr not in self.permAccepts:
            self.permAccepts[self.ctr] = {}
        for nodeId in range(self.paxosConfig['nodes']):
            suggId = self.constructSuggId()
            print("\033[92m%d -> %d perm %s \033[0m" %(fromNodeId, nodeId, suggId))
            self.comm.sendMesg(self.nodeId, nodeId, self.PermissionMessage(suggId))
    
    # Proposer Function
    def txProposal(self, fromNodeId, suggId, mesgVal):
        if self.ctr not in self.proposalAccepts:
            self.proposalAccepts[self.ctr] = {}
        for nodeId in range(self.paxosConfig['nodes']):
            print("\033[92m%d -> %d proposal %s :: %s \033[0m" %(fromNodeId, nodeId, suggId, mesgVal))
            self.comm.sendMesg(self.nodeId, nodeId, {
                'data': {
                    'type': 'propose',
                    'suggestionId': suggId,
                    'value': mesgVal
                },
                'protocol': 'paxos'
            })

    # Proposer Function
    def txAccepted(self, fromNodeId, suggId, mesgVal):
        for nodeId in range(self.paxosConfig['nodes']):
            print("\033[92m%d -> %d accepted %s :: %s \033[0m" %(fromNodeId, nodeId, suggId, mesgVal))
            self.comm.sendMesg(self.nodeId, nodeId, {
                'data': {
                    'type': 'accepted',
                    'suggestionId': suggId,
                    'value': mesgVal
                },
                'protocol': 'paxos'
            })



    # Proposer Function
    def rxPermAccept(self, fromNodeId, toNodeId, suggId):
        print("%d <- %d perm_accept %s" %(toNodeId, fromNodeId, suggId))
        suggIdCtr = int(suggId.strip('<').strip('>').split(',')[0])
        self.permAccepts[suggIdCtr][fromNodeId] = True
        if suggIdCtr == self.ctr and len(self.permAccepts[suggIdCtr].keys()) == (self.paxosConfig['nodes']//2):
            print("%d: PERM ACCEPTED MAJORIY %s" %(self.nodeId, suggId))
            self.txProposal(self.nodeId, suggId, self.mesgVal)



    # Proposer Function
    def rxPermDeny(self, fromNodeId, toNodeId, suggId, lastPromised, lastAccepted, lastAcceptedVal):
        print("%d <- %d perm_deny %s" %(toNodeId, fromNodeId, suggId))
        suggIdCtr = int(lastPromised.strip('<').strip('>').split(',')[0])
        if suggIdCtr >= self.ctr:
            self.ctr = suggIdCtr + 1
            print("Node %d Retrying Perm" %(self.nodeId))
            if lastAcceptedVal != None:
                self.mesgVal = lastAcceptedVal
            # time.sleep(2*random.random())
            self.txPerm(self.nodeId)


    
    
    def rxProposalAccept(self, fromNodeId, toNodeId, suggId):
        print("%d <- %d proposal_accept %s" %(toNodeId, fromNodeId, suggId))
        suggIdCtr = int(suggId.strip('<').strip('>').split(',')[0])
        self.proposalAccepts[suggIdCtr][fromNodeId] = True
        if suggIdCtr == self.ctr:
            if len(self.proposalAccepts[suggIdCtr].keys()) == (self.paxosConfig['nodes']//2):
                print("%d: PROPOSAL ACCEPTED MAJORIY %s" %(self.nodeId, suggId))
                self.txAccepted(self.nodeId, suggId, self.mesgVal)
            
    
    
    # Proposer Function
    def rxProposalDeny(self, fromNodeId, toNodeId, suggId, lastPromised, lastAccepted, lastAcceptedVal):
        print("%d <- %d proposal_deny %s :: LP: %s :: LA: %s :: LAV: %s" %(toNodeId, fromNodeId, suggId, lastPromised, lastAccepted, lastAcceptedVal))
        
        lastPromisedCtr = int(lastPromised.strip('<').strip('>').split(',')[0]) if lastPromised != None else -1
        lastAcceptedCtr = int(lastAccepted.strip('<').strip('>').split(',')[0]) if lastAccepted != None else -1
        biggestCtr = max(lastPromisedCtr, lastAcceptedCtr)
        
        if biggestCtr >= self.ctr:
            self.ctr = biggestCtr + 1
            print("%d - Retrying Perm (from proposal)" %(self.nodeId))
            if lastAcceptedVal != None:
                self.mesgVal = lastAcceptedVal
            self.txPerm(self.nodeId)
        

    
    
    # Acceptor Function
    def rxPerm(self, fromNodeId, toNodeId, suggId):
        print("%d <- %d perm %s" %(toNodeId, fromNodeId, suggId))
        if self.lastPromised == None or suggId >= self.lastPromised:
            print("\033[92m%d -> %d perm_accept %s \033[0m" %(toNodeId, fromNodeId, suggId))
            self.lastPromised = suggId
            self.comm.sendMesg(self.nodeId, fromNodeId, {
                'data': {
                    'type': 'permission_accept',
                    'suggestionId': suggId,
                    'last_promised': self.lastPromised,
                    'last_accepted': self.lastAccepted,
                    'last_accepted_value': self.lastAcceptedValue,
                },
                'protocol': 'paxos'
            })
        else:
            print("\033[92m%d -> %d perm_deny %s \033[0m" %(fromNodeId, toNodeId, suggId))
            self.comm.sendMesg(self.nodeId, fromNodeId, {
                'data': {
                    'type': 'permission_deny',
                    'suggestionId': suggId,
                    'last_promised': self.lastPromised,
                    'last_accepted': self.lastAccepted,
                    'last_accepted_value': self.lastAcceptedValue,
                },
                'protocol': 'paxos'
            })
    
    def rxProposal(self, fromNodeId, toNodeId, suggId, mesgVal):
        print("%d <- %d proposal %s :: %s" %(toNodeId, fromNodeId, suggId, mesgVal))
        if self.lastPromised == None or ((self.lastPromised == None or (self.lastPromised != None and suggId >= self.lastPromised)) and (self.lastAccepted == None or (self.lastAccepted != None and suggId >= self.lastAccepted))):
            # print("Node %d Recieved Proposal. Accepted %s > %s and > %s from Node %d" %(toNodeId, suggId, self.lastPromised, self.lastAccepted, fromNodeId))
            self.lastAccepted = suggId
            self.lastAcceptedValue = mesgVal
            print("\033[92m%d -> %d proposal_accept %s \033[0m" %(toNodeId, fromNodeId, suggId))
            self.comm.sendMesg(self.nodeId, fromNodeId, {
                'data': {
                    'type': 'proposal_accept',
                    'suggestionId': suggId,
                    'value': mesgVal,
                    'last_promised': self.lastPromised,
                    'last_accepted': self.lastAccepted,
                    'last_accepted_value': self.lastAcceptedValue,
                },
                'protocol': 'paxos'
            })
        else:
            print("\033[92m%d -> %d proposal_deny %s \033[0m" %(fromNodeId, toNodeId, suggId))
            self.comm.sendMesg(self.nodeId, fromNodeId, {
                'data': {
                    'type': 'proposal_deny',
                    'suggestionId': suggId,
                    'last_promised': self.lastPromised,
                    'last_accepted': self.lastAccepted,
                    'last_accepted_value': self.lastAcceptedValue,
                },
                'protocol': 'paxos'
            })
            
    def rxAccepted(self, fromNodeId, toNodeId, suggId, mesgVal):
        print("\033[92m%d CONSENSUS %s :: %s \033[0m" %(toNodeId, suggId, mesgVal))
        self.consensusValue = mesgVal;
    


class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'