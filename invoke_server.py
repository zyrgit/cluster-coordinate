#!/usr/bin/env python

import os, sys
import subprocess
import random, time
import threading

import inspect
mypydir =os.path.abspath(os.path.dirname(inspect.getfile(inspect.currentframe())))
sys.path.append(mypydir+'/mypy')
from hostip import ip2host
import monitor

if __name__ == "__main__":

	delimit='~'

	params=sys.argv[1]
	if params=='':
		print 'wrong! no params as invoke cmd ! '
		sys.exit(1)
	cmd=params.strip().split(delimit)

	# Popen do not wait and block here! output file will be in HOME if not abs dir
	#os.popen(cmd) # will block
	subprocess.Popen(cmd, stdout=subprocess.PIPE) # not block if stdout is contained within subproc
	
	# FIND MY IP	
	grep = subprocess.Popen('hostname -s'.split(), stdout=subprocess.PIPE)
	output = grep.communicate()[0]
	if 'Mac' in output:
		findip = subprocess.Popen('ipconfig getifaddr en0'.split(), stdout=subprocess.PIPE)
		myip = findip.communicate()[0].rstrip()
	else:
		findip = subprocess.Popen('hostname -I'.split(), stdout=subprocess.PIPE)
		myip = findip.communicate()[0].rstrip()
	if myip=='':
		myip=monitor.get_ip_addr()
	ipsufix=myip.split('.')[-1].rstrip()
	tname=ip2host[myip]

	print params+"     DONE! "+tname
