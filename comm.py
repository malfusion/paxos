class Communicator():
    def __init__(self, nodes):
        self.network = {}
        for node in nodes:
            self.network[node.getId()] = node
    
    def sendMesg(self, fromNodeId, toNodeId, mesg):
        if toNodeId not in self.network:
            raise BaseException('Node ' + str(toNodeId) + ' not found')
        
        mesg['metadata'] = {}
        mesg['metadata']['to'] = toNodeId
        mesg['metadata']['from'] = fromNodeId
        self.network[toNodeId].onMesgDelivery(mesg)
        