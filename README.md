# cluster-coordinate
Master-Slave cluster task coordination using Python. The communication between servers is realized using RPC. 

## Function
On slave-machines, run webtest_worker.py. On master-machine, run webtest_coor.py. The master will using SSH to invoke slave-machines, doing sync and complete tasks. 
