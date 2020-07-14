
Paxos 
===============
Introduction
-----------------

A multithreaded implementation of the Paxos algorithm to achieve consensus on a single value
The aim of the repository is to establish a good understanding of the algorithm by implementing it from scratch
and also to create an architecture which will form the backbone of further more complicated dsistributed systems projects.


Python Installation
-------------------
Use a conda environment for easy setup. Change the config file to modify the topology of the network.

```bash
$ conda env create -f environment.yml
$ conda activate paxos
$ python paxos
```
