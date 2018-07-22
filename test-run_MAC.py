#!/usr/bin/env python

import os, sys, threading
import subprocess
import random, time
import inspect
mypydir =os.path.abspath(os.path.dirname(inspect.getfile(inspect.currentframe())))
sys.path.append(mypydir+'/mypy')
import readconf
from hostip import host2userip
from servicedir import host2dir

def th_sh(h,script):
	cmd="ssh -tt "+host2userip[h]+ " sh "+host2dir(h)+"/"+script
	print '\nrun: '+cmd
	subprocess.call(cmd,shell=True)

if __name__ == "__main__":
	tt=os.path.getmtime('configure_t')
	te=os.path.getmtime('configure_e')
	print 'configure_t last modified time:'+`tt`
	print 'configure_t last modified time:'+`te`

	if tt>te: # which is newer?
		et='t'
	else:
		et='e'

	if len(sys.argv)>1: # or override by cmd
		et=sys.argv[1]

	print 'test on  '+et
	time.sleep(1)

	coor=readconf.get_hostlist(et,'coor')[0]
	allhosts=readconf.get_allhosts(et)
	thqueue=[]
	for h in allhosts:
		t=threading.Thread(target=th_sh,args=(h,'kill_process.sh'))
		t.setDaemon(True)
		t.start()
		thqueue.append(t)

	# gen php file first:
	cmd='python ./gen-script_MAC.py '+et
	print cmd
	subprocess.call(cmd.split())

	print 'copy...'
	
	cmd='python ./cp_to_MAC.py '+et
	print '\n\n\nrun: '+cmd
	subprocess.call(cmd.split())
	time.sleep(1)

	for th in thqueue:
		th.join()
	

	userip=host2userip[coor]
	cmd="ssh -tt "+userip+ " 'python "+host2dir(coor)+"/webtest_coor.py "+et+"'"
	print '\n\n\nrun: '+cmd
	subprocess.call(cmd,shell=True) # has to shell, or no such file...
	time.sleep(1)

	cmd='python ./col_res_MAC.py '+et
	print '\n\n\nrun: '+cmd
	subprocess.call(cmd.split())

#t make sure mysqld, lighttpd, haproxy, php-fpm, memcached 
#e make sure mysql, lighttpd, haproxy, php5-fpm, memcached