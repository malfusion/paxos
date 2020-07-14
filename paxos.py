import threading

class Paxos:
    def __init__(self, paxosNodes):
        self.nodes = paxosNodes



class PaxosNode():
    def __init__(self, nodeId, paxosConfig, comm):
        self.paxosConfig = paxosConfig
        self.nodeId = nodeId
        self.comm = comm

        #Proposer
        self.mesgVal = None
        self.ctr = 0
        self.permAccepts = {}

        #Acceptor
        self.lastPromised = None
        self.lastAccepted = None
        

    def processMesg(self, mesg):
        if mesg['data']['type'] == 'permission_accept':
            self.rxPermAccept(mesg['metadata']['from'], mesg['metadata']['to'], mesg['data']['suggestionId'])
        if mesg['data']['type'] == 'permission_deny':
            self.rxPermDeny(mesg['metadata']['from'], mesg['metadata']['to'], mesg['data']['suggestionId'])
        if mesg['data']['type'] == 'permission':
            self.rxPerm(mesg['metadata']['from'], mesg['metadata']['to'], mesg['data']['suggestionId'])
        
    def sync(self, value):
        self.mesgVal = value
        # TO REMOVE
        if self.nodeId < 2:
            self.txPerm(self.nodeId)

    # Proposer Function
    def txPerm(self, fromNodeId):
        if self.ctr not in self.permAccepts:
            self.permAccepts[self.ctr] = {}
        for nodeId in range(self.paxosConfig['nodes']):
            self.comm.sendMesg(self.nodeId, nodeId, {
                'data': {
                    'type': 'permission',
                    'suggestionId': '<' + str(self.ctr) + ',' + str(self.nodeId) + '>'
                },
                'protocol': 'paxos'
            })

    # Proposer Function
    def rxPermAccept(self, fromNodeId, toNodeId, suggId):
        print("Node %d Recieved permission_accept %s from Node %d" %(toNodeId, suggId, fromNodeId))
        suggIdCtr = int(suggId.strip('<').strip('>').split(',')[0])
        self.permAccepts[suggIdCtr][fromNodeId] = {
            'suggestionId': suggId
        }
        if suggIdCtr == self.ctr and len(self.permAccepts[suggIdCtr].keys()) == (self.paxosConfig['nodes']//2):
            print("Node %d: Permission %s got accepted, sending proposal" %(self.nodeId, suggId))
            # self.txProposal(fromNodeId, self.mesgVal)

    # Proposer Function
    def rxPermDeny(self, fromNodeId, toNodeId, suggId):
        print("Node %d Recieved permission_deny %s from Node %d" %(toNodeId, suggId, fromNodeId))
        suggIdCtr = int(suggId.strip('<').strip('>').split(',')[0])
        if suggIdCtr > self.ctr:
            self.ctr = suggIdCtr + 1
            print("Node %d Retrying with higher suggestion id: %d" %(self.nodeId, self.ctr))
            self.txPerm(self.nodeId)
        

    # Acceptor Function
    def rxPerm(self, fromNodeId, toNodeId, suggId):
        print("Node %d Recieved permission %s from Node %d" %(toNodeId, suggId, fromNodeId))
        if self.lastPromised == None or suggId > self.lastPromised:
            print("Node %d Recieved. Accepted %s > %s from Node %d, threadid: %s" %(toNodeId, suggId, self.lastPromised, fromNodeId, threading.get_ident()))
            self.lastPromised = suggId
            self.comm.sendMesg(self.nodeId, fromNodeId, {
                'data': {
                    'type': 'permission_accept',
                    'suggestionId': suggId
                },
                'protocol': 'paxos'
            })
        else:
            print("Node %d Denied permission %s from Node %d" %(toNodeId, suggId, fromNodeId))
            self.comm.sendMesg(self.nodeId, fromNodeId, {
                'data': {
                    'type': 'permission_deny',
                    'suggestionId': suggId,
                    'last_promised': self.lastPromised
                },
                'protocol': 'paxos'
            })
            
    