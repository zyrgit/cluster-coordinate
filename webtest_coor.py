#!/usr/bin/env python

import os, sys, traceback
import subprocess
import random, time
import urllib2
import threading, logging
from SimpleXMLRPCServer import SimpleXMLRPCServer
import xmlrpclib

import inspect
mypydir =os.path.abspath(os.path.dirname(inspect.getfile(inspect.currentframe())))
sys.path.append(mypydir+'/mypy')
from readconf import get_hostlist,get_allhosts

from hostip import host2ip, ip2host, host2userip
from servicedir import host2dir,host2servicedict  
import monitor,readconf
from monitor import get_power #(hoststr,suffix,LOG)

delimit='~'

def invoke_server(userip, cmd): 
	#ssh do not use -tt here
	cmd=cmd.replace(' ',delimit)
	cmd='ssh '+userip+' python '+host2dir(userip)+'/invoke_server.py '+cmd
	subprocess.call(cmd,shell=True) # call has to block and wait here
	print "coor cmd: "+cmd

def use_thread_invoke_server(userip, cmd):
	t=threading.Thread(target=invoke_server,args=(userip, cmd))
	t.setDaemon(True)
	t.start()
	return t

def th_set_params(host,paramsdict):
	server='http://'+host2ip[host]+':'+masterport
	print 'coor connecting rpc to: '+host+' on thread '+threading.currentThread().getName()
	try:
		proxy = xmlrpclib.ServerProxy(server)
		proxy.set_params(paramsdict)
		print 'coor th_set_params done on '+host
	except:
		print 'th_set_params.exception on '+host+' on thread '+threading.currentThread().getName()
		logger.error(str(traceback.format_exc()))

def th_set_runflag(host,run):
	try:
		proxy = xmlrpclib.ServerProxy('http://'+host2ip[host]+':'+masterport)
		proxy.set_runflag(int(run))
	except:
		print 'th_set_runflag.exception on '+host+' on thread '+threading.currentThread().getName()
		logger.error(str(traceback.format_exc()))

def th_set_genserver_runtime(host,runtime):
	try:
		proxy = xmlrpclib.ServerProxy('http://'+host2ip[host]+':'+masterport)
		proxy.set_genserver_runtime(int(runtime))
		print 'Done! th_set_genserver_runtime '+`runtime`
	except:
		print 'set_genserver_runtime.exception on '+host
		logger.error(str(traceback.format_exc()))

def th_heartbeat(host,msg):
	proxy = xmlrpclib.ServerProxy('http://'+host2ip[host]+':'+masterport)
	ret=proxy.heartbeat(msg)
	print 'heartbeat ',ret

def from_worker_to_coor(paramsdict):
	global waitlist,runflag,finishlist,syncdict,workererror
	if 'ready' in paramsdict.keys():
		host=str(paramsdict['ready'])
		while host in waitlist:
			waitlist.remove(host)
		if host not in syncdict.keys():
			syncdict[host]={'who':host}
		print host+' is ready...'
	if 'finish' in paramsdict.keys():
		if not paramsdict['finish'] in finishlist:
			finishlist.append(paramsdict['finish'])

	if 'synclock' in paramsdict.keys():
		h=str(paramsdict['who'])
		syncdict[h]['synclock']=int(paramsdict['synclock'])
	
	if 'workererror' in paramsdict.keys():
		workererror=str(paramsdict['workererror'])
		if workererror!='': print '\nERROR :'+workererror

def rpc_server_thread(): # rpc handle request, while serverruntime timer still
	global masterport,myip
	SimpleXMLRPCServer.allow_reuse_address = 1
	server = SimpleXMLRPCServer((myip,int(masterport)),allow_none=True,logRequests=False)
	server.register_function(from_worker_to_coor)
	server.serve_forever()
	print tname+' '+nametag+" RPC server_thread setup done"

def th_rpc_work_start(host,cmd):
	try:
		proxy = xmlrpclib.ServerProxy('http://'+host2ip[host]+':'+masterport)
		proxy.work_start(int(cmd))
	except:
		print 'th_rpc_work_start.exception on '+host+' on th '+threading.currentThread().getName()
		logger.error(str(traceback.format_exc()))

if __name__ == "__main__":

# init
	et=sys.argv[1]
	if et!='e' and et!='t':
		print 'webtest wrong e t?'
		sys.exit(1)

	MANUALINVOKE=0
	runflag=1
	nametag=''
	finishlist=[]
	syncdict={}
	synclock=0
	lastsync=0
	workererror=''
# get list
	masterlist=get_hostlist(et,'master') # read conf, could be []
	print 'masterlist:'
	print masterlist,len(masterlist)

	slavelist=get_hostlist(et,'slave') # read conf, could be empty
	print 'slavelist:'
	print slavelist,len(slavelist)

	coorlist=get_hostlist(et,'coor')
	print 'coorlist:',coorlist

	allhosts=get_allhosts(et)
	print 'all hosts:',allhosts

	waitlist=allhosts[:]
	for h in coorlist:
		while h in waitlist:
			waitlist.remove(h)
	workerlist=waitlist[:]
# FIND IP , tname
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
	print ' coor ip sufix: '+ipsufix

# get conf
	masterport=readconf.get_conf(et,'masterport') # rpc
	usecache=readconf.get_conf(et,'usecache')
	usemysql=readconf.get_conf(et,'usemysql')
	writefile=readconf.get_conf(et,'writefile')
	sampletime=readconf.get_conf(et,'sampletime')
	hitratio=readconf.get_conf(et,'hitratio')
	newMonitorFile=readconf.get_conf(et,'newMonitorFile') # if rewrite monitor.log
	cpuMeasIntv=readconf.get_conf(et,'cpuMeasIntv')  # measure cpu block intv time
	monitorSampleTime=readconf.get_conf(et,'monitorSampleTime') # psutil sample t
	testtime=int(readconf.get_conf(et,'testtime'))
	rpctimeout=int(readconf.get_conf(et,'servertimeout'))
	skipservicecheck=int(readconf.get_conf(et,'skipservicecheck'))
	skipwarmup=int(readconf.get_conf(et,'skipwarmup'))
	cleanlog=int(readconf.get_conf(et,'cleanlog'))
	cleanres=int(readconf.get_conf(et,'cleanres'))
	manualsetrun=int(readconf.get_conf(et,'manualsetrun'))
	
	if nametag=='':
		if tname in masterlist:
			nametag='master'
		elif tname in slavelist:
			nametag='slave'
		elif tname in coorlist:
			nametag='coor'
		else:
			print 'what is my role ???'
			sys.exit(1)
		print 'my role: '+nametag

# START RPC SERVER
	tgen = threading.Thread(target=rpc_server_thread)
	tgen.setDaemon(True) # when main exit, this thread also terminate
	tgen.start()
	print tname+"  RPC server thread running... "+nametag

# INVOKE REMOTE 
	thqueue=[]
	MANUALINVOKE=int(readconf.get_conf(et,'manualinvoke'))
	if MANUALINVOKE==0:
		print "coor invoking python servers..."
		for n in workerlist: # nametag at last
			cmd='python '+host2dir(n)+'/webtest_worker.py '+et
			thqueue.append(use_thread_invoke_server(host2userip[n],cmd))
		# wait worker to start RPC...
		while len(thqueue)>0:
			t=thqueue.pop(0)
			print 'coor: join th invoke_server %s' % t.getName() 
			t.join() # make sure every work has started RPC
		time.sleep(1)
	else:
		print '\n\nYou need to invoke rpc on servers yourself! '
		time.sleep(3)

# cleaning
	if cleanlog>=1:
		cmd='ssh -tt '+host2userip[tname]+' sh '+host2dir(tname)+'/clean_log.sh'
		print '\nclean cmd: '+cmd
		subprocess.call(cmd.split())
	if cleanres>=1:
		cmd='ssh -tt '+host2userip[tname]+' sh '+host2dir(tname)+'/clean_res.sh'
		print '\nclean cmd: '+cmd
		subprocess.call(cmd.split())

# SET LOG, not for storing results.
	logging.basicConfig(level=logging.INFO)
	logger = logging.getLogger(__name__)
	handler = logging.FileHandler(host2dir(tname)+'/'+nametag+'-'+tname+'.log')
	handler.setLevel(logging.INFO)
	formatter = logging.Formatter('%(asctime)s %(name)s %(levelname)s: %(message)s')
	handler.setFormatter(formatter)
	logger.addHandler(handler)
	
# wait for every worker	
	try:
		# wait worker ..
		while len(waitlist)>0:
			print nametag+'  waiting for ',waitlist
			time.sleep(1) # make sure every worker is ready
# sync clock with workers:
		thqueue=[]
		for n in workerlist:
			print 'coor synclock on ' +n
			t = threading.Thread(target=th_set_params,args=(n,{'synclock':synclock}))
			t.setDaemon(True)
			t.start()
			thqueue.append(t)
		while len(thqueue)>0:
			thqueue.pop(0).join() 
		logger.info('spread synclock %d'%synclock)
# RPC RUN now
		if manualsetrun>0:
			inp=raw_input('>>> start run ? ')

		for n in workerlist:
			print 'coor set_runflag 1 on ' +n
			t = threading.Thread(target=th_set_runflag,args=(n,1))
			t.setDaemon(True)
			t.start()
		logger.info('set run flag on workers after sync %d'%synclock)

# initial sync
		sync=0
		while sync==0:
			sync=1
			for h in workerlist:
				if ('synclock' not in syncdict[h].keys()) or (syncdict[h]['synclock']!=synclock):
					sync=0
					print 'sync %d wait for %s'%(synclock,h)
					time.sleep(1)
					break
		synclock+=1
		logger.info('collected sync, advance to %d'%synclock)

		thqueue=[]
		for n in workerlist:
			t = threading.Thread(target=th_set_params,args=(n,{'logsuffix':'0','synclock':synclock}))
			t.setDaemon(True)
			t.start()
			thqueue.append(t)
		while len(thqueue)>0:
			thqueue.pop(0).join() 
		for n in workerlist:
			t = threading.Thread(target=th_rpc_work_start,args=(n,1))
			t.setDaemon(True)
			t.start()
		logger.info('spread synclock %d, plus work start'%synclock)

# RUN GEN TEST
		print 'coor entering while loop...'
		starttime=time.time()
		lastshowtime=starttime
		lastpowertime=starttime
		endtime=starttime+int(readconf.get_conf(et,'testtime'))+20
		lastsynctime=starttime
		
		while time.time()<endtime and runflag:
			if time.time()-lastshowtime>=5:
				lastshowtime=time.time()
				print '...test still %.1f to go...'%(endtime-time.time())
			sync=1
			for h,dic in syncdict.items():
				if dic['synclock']!=synclock:
					sync=0
					#print 'waiting for '+h
			if sync==0:
				if time.time()-lastsynctime>300:
					msg= '\n\n sync wait too long, coor sys exit...\n\n'
					print msg
					logger.info(msg)
					runflag=0
					break
			if sync>0:
				lastsynctime=time.time()
				synclock+=1
				thqueue=[]
				for n in workerlist: # set next sync clock
					t = threading.Thread(target=th_set_params,args=(n,{'synclock':synclock}))
					t.setDaemon(True)
					t.start()
					thqueue.append(t)
				for th in thqueue:
					th.join()
###### COOR DO SOMETHING EVERY TIME WORKER SYNC here:
				print 'coor do something...'


				for n in workerlist: # determine if job done?
					t = threading.Thread(target=th_set_params,args=(n,{'jobdone':1}))
					t.setDaemon(True)
					t.start()
###### AFTER DONE ABOVE,  START WORKER:
				for n in workerlist:
					t = threading.Thread(target=th_rpc_work_start,args=(n,1))
					t.setDaemon(True)
					t.start()
			if len(finishlist)>=len(workerlist):
				print 'all finish, exit...'
				runflag=0
				break
			elif len(finishlist)>0:
				print 'who has finished: ',finishlist
				#runflag=0

			if workererror!='':
				print '\nERROR: '+workererror
			time.sleep(1) 
# COOR OUT OF WHILE LOOP, FINISH
		for n in workerlist:
			if n in finishlist:
				continue
			print 'coor set_runflag 0 on ' +n
			t = threading.Thread(target=th_set_runflag,args=(n,0))
			t.setDaemon(True)
			t.start()
		print "coor success, now terminating..."
	except:
		print " coor FAIL on exception:"
		traceback.print_exc()
		logger.error(str(traceback.format_exc()))
# TERMINATE GEN SERVER
	finally:
		logger.info('coor finally exits')

# EXIT
	main_thread = threading.currentThread()
	for t in threading.enumerate():
	    if t is main_thread:
	        continue
	    print 'coor exit joining %s' % t.getName() 
	    t.join(10) # if timeout, left to daemon kill.
	
	print " coor __main__ exit "

