import sys
import yaml
from paxos import Paxos, PaxosNode
from node import Node
from comm import Communicator

class Simulation():
    def __init__(self, confFile, baseDir):
        configFile = 'config.yaml'
        self.baseDir = baseDir
        if confFile:
            configFile = confFile
        with open(configFile) as f:
            self.config = yaml.load(f, Loader=yaml.FullLoader)

    def run(self):
        
        nodes = []
        for i in range(self.config['nodes']):
            nodes.append(Node(i))
        
        comm = Communicator(nodes)

        for node in nodes:
            node.initPaxos(nodes, comm, self.config)
            node.start()

        
        for node in nodes:
            node.join()
        
        # paxos = Paxos(nodes)





if __name__ == "__main__":
    if(len(sys.argv)>1):
        Simulation(sys.argv[1], "./").run()
    else:
        print("Please execute using: python simulation.py <config.yaml>")