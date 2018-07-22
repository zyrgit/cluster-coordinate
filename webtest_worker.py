#!/usr/bin/env python

import os, sys, traceback
import subprocess
import random, time
import urllib2
import threading
from SimpleXMLRPCServer import SimpleXMLRPCServer
import xmlrpclib

import inspect
mypydir =os.path.abspath(os.path.dirname(inspect.getfile(inspect.currentframe())))
sys.path.append(mypydir+'/mypy')
from readconf import get_hostlist,get_allhosts
from monitor import get_power #(hoststr,suffix,LOG)

from hostip import host2ip, ip2host, host2userip
from servicedir import host2dir,host2servicedict,host2type
import logging,glob
try:
	import pylibmc
except:
	print 'no pylibmc!'
import readconf,monitor
from signal import signal, SIGPIPE, SIG_DFL 

monitordict={'mem':1,'cpu':1,'nettxrx':1,'netconn':1,'conn2web':1,'conn2mem':1,'conn2sql':1,'conn2gen':1}

totalsize=0

def heartbeat(msg_echo):
	global serverruntime
	echo_msg= ipsufix+': '+str(msg_echo)+' remtime='+`serverruntime`
	print tname+' '+nametag+" recv heartbeat "+`serverruntime`
	return echo_msg

def th_url_run(url,lock,fname):
	global delaytime,ipsufix,totalsize
	req = urllib2.Request(url)
	sttime=time.time()
	res = urllib2.urlopen(req)
	rf=res.read()
	sz=sys.getsizeof(rf)
	totalsize+=sz
	restr=('%.4f' % (time.time()-sttime))
	if lock==None or fname=='':
		return
	# append, file lock
	lock.acquire()
	try:
		resf=open(fname,'a')
		resf.write(restr+' '+`sz`+' '+('%.4f'%sttime)+' '+('%.4f'%time.time())+'\n')
	except:
	    logging.exception(tname+" resf failed! "+nametag)
	finally:
		resf.close()
		lock.release()

def set_runflag(int_run): # used for start running sync
	global runflag
	runflag=int_run
	print tname+' '+nametag+" recv runflag "+`runflag`

def set_genserver_runtime(rpcrun): # set rpc server timer
	global serverruntime,lastRPCrefreshtime,endtime
	lastRPCrefreshtime=time.time()
	serverruntime=rpcrun
	endtime=float(rpcrun)+time.time()
	print tname+' '+nametag+" recv runtime "+`serverruntime`

def set_params(paramdict):
	global usemysql,usecache,writefile,sampletime,hitratio,delaytime,rqps
	global rewrite,sampleIntv,measureIntv,monitordict,powersuffix,monitorsuffix
	global tuned,mincall,maxcall,synclock,jobdone
	if 'cache' in paramdict.keys():
		usecache=int(paramdict['cache'])
	if 'sql' in paramdict.keys():
		usemysql=int(paramdict['sql'])
	if 'file' in paramdict.keys():
		writefile=int(paramdict['file'])
	if 'sample' in paramdict.keys():
		sampletime=float(paramdict['sample'])
	if 'hit' in paramdict.keys():
		hitratio=float(paramdict['hit'])
	if 'delay' in paramdict.keys():
		delaytime=float(paramdict['delay'])
	if 'rqps' in paramdict.keys():
		rqps=int(paramdict['rqps'])
	if 'rewrite' in paramdict.keys():
		rewrite=int(paramdict['rewrite'])
	if 'sampleIntv' in paramdict.keys():
		sampleIntv=float(paramdict['sampleIntv'])
	if 'measureIntv' in paramdict.keys():
		measureIntv=float(paramdict['measureIntv'])
	if 'mon_mem' in paramdict.keys():
		monitordict['mem']=int(paramdict['mon_mem'])
	if 'mon_cpu' in paramdict.keys():
		monitordict['cpu']=int(paramdict['mon_cpu'])
	if 'mon_nettxrx' in paramdict.keys():
		monitordict['nettxrx']=int(paramdict['mon_nettxrx'])
	if 'mon_netconn' in paramdict.keys():
		monitordict['netconn']=int(paramdict['mon_netconn'])
	if 'mon_conn2web' in paramdict.keys():
		monitordict['conn2web']=int(paramdict['mon_conn2web'])
	if 'mon_conn2mem' in paramdict.keys():
		monitordict['conn2mem']=int(paramdict['mon_conn2mem'])
	if 'mon_conn2sql' in paramdict.keys():
		monitordict['conn2sql']=int(paramdict['mon_conn2sql'])
	if 'mon_conn2gen' in paramdict.keys():
		monitordict['conn2gen']=int(paramdict['mon_conn2gen'])
	if 'logsuffix' in paramdict.keys():
		powersuffix=str(paramdict['logsuffix'])
		monitorsuffix=str(paramdict['logsuffix'])
	if 'synclock' in paramdict.keys():
		synclock=int(paramdict['synclock'])
		print tname+' recv synclock: ',synclock
	if 'jobdone' in paramdict.keys():
		jobdone=int(paramdict['jobdone'])
		print tname+' recv job done: ',jobdone

	print tname+' '+nametag+" recv params: ",paramdict

def rpc_server_thread(): # rpc handle request, while serverruntime timer still
	global serverruntime,masterport,myip
	SimpleXMLRPCServer.allow_reuse_address = 1
	server = SimpleXMLRPCServer((myip,int(masterport)),allow_none=True,logRequests=False)
	server.register_function(set_runflag)
	server.register_function(heartbeat)
	server.register_function(set_genserver_runtime)
	server.register_function(set_params)
	server.register_function(work_start)
	server.serve_forever()
	print tname+' '+nametag+" RPC server_thread exit"

def th_monitor_run(rewrite,sampleIntv,measureIntv=0.5):
	global serverruntime,nametag,runflag,monitordict,monitorsuffix
	global masterlist,slavelist,allhosts
	if rewrite:
		try:
			fn=glob.glob(host2dir(tname)+'/monitor*')
			for rmf in fn:
				os.remove(rmf)
		except:
			print 'monitor.log already removed !'
	print tname+" "+nametag+' thread running...\nmonitordict:',monitordict,sampleIntv
	while serverruntime>0 and runflag:
		fid=open(host2dir(tname)+'/monitor-'+monitorsuffix+'-'+nametag+'-'+tname+'.log','a')
		sttime=time.time()
		if monitordict['cpu']>0:
			core=monitor.get_cpu_count() # cpu logical number
			dic=monitor.get_cpu_usage(intv=measureIntv) # us, sy, % each core
			msg='cpu'
			for i in range(core):
				msg=msg+' %.1f,%.1f'%(dic[i]['usr'],dic[i]['sys'])
			fid.write(('%.4f'%time.time())+' '+msg+'\n')
		if monitordict['mem']>0:
			dic=monitor.get_mem_usage() # used %, total MB
			msg='mem %.1f %d'%(dic['used'],dic['total'])
			fid.write(('%.4f'%time.time())+' '+msg+'\n')
		if monitordict['nettxrx']>0:
			dic=monitor.get_net_txrx(rlevel=4) # tx,rx,dropin,dropout
			msg='nettxrx %.4f %.4f %d %d %d %d %d %d'%(dic['tx'],dic['rx'],dic['dropin'],dic['dropout'],dic['errin'],dic['errout'],dic['txpac'],dic['rxpac'])
			fid.write(('%.4f'%time.time())+' '+msg+'\n')
		if nametag=='mem':
			pname=host2servicedict(tname)['mem'][0] # memcached proc num
			dic=monitor.get_procnum(pname)
			fid.write(('%.4f'%time.time())+' '+pname+' '+`dic[pname]`+'\n')
		if nametag=='web':
			pname=host2servicedict(tname)['web'][0] # php-fpm proc num
			dic=monitor.get_procnum(pname)
			fid.write(('%.4f'%time.time())+' '+pname+' '+`dic[pname]`+'\n')
			pname=host2servicedict(tname)['web'][1] # memcached proc num
			dic=monitor.get_procnum(pname)
			fid.write(('%.4f'%time.time())+' '+pname+' '+`dic[pname]`+'\n')
		if monitordict['netconn']>0:
			dic=monitor.get_net_conn() # {rip:{CONN_TIME_WAIT:1000, }, }
			for rip in dic.keys():
				if rip in ip2host.keys():
					tn=ip2host[rip]
					ripdic=dic[rip]
					if tn in masterlist:
						msg='to-master '
					elif tn in slavelist:
						msg='to-slave '
					elif tn in coorlist:
						msg='to-coor '
					else:
						msg='to-? '
					msg=msg+tn
					for key in ripdic:
						msg=msg+' '+key+' '+str(ripdic[key])
					fid.write(('%.4f'%time.time())+' '+msg+'\n')
		fid.close()
		endtime=time.time()-sttime
		time.sleep(max(0,sampleIntv-endtime))
	print tname+' mon thread exit.'+nametag

def th_get_power(suffix):
	global powersuffix
	lastpowertime=time.time()
	while runflag:
		if time.time()-lastpowertime>5:
			lastpowertime=time.time()
			if tname.startswith('t'):
				power=get_power(tname,powersuffix+'-'+suffix,1)
				#print tname+' power:'+str(power)
		time.sleep(1)

def work_start(command):
	global workstart
	workstart=int(command)
	print tname+" "+nametag+' recv work_start '+str(command)

if __name__ == "__main__":

	# 'gen' 'web' 'mem' 'sql' ?
	nametag=''
	powersuffix=''
	monitorsuffix=''
	synclock=-1
	jobdone=0
	musttellcoor=0

	et=sys.argv[1]
	if et!='e' and et!='t':
		print 'webtest worker wrong e t?'
		sys.exit(1)

	#signal(SIGPIPE,SIG_DFL) # broken pipe.
	
	runflag=0
# get hosts
	# masterlist=get_hostlist(et,'master') # read conf, could be []
	# print 'masterlist:'
	# print masterlist,len(masterlist)

	# slavelist=get_hostlist(et,'slave') # read conf, could be empty
	# print 'slavelist:'
	# print slavelist,len(slavelist)

	# coorlist=get_hostlist(et,'coor')
	# print 'coorlist:',coorlist

	# allhosts=get_allhosts(et)
	# print 'all hosts:',allhosts
# get hosts
	weblist=get_hostlist(et,'web') # read conf, could be []
	print 'weblist:'
	print weblist,len(weblist)

	genlist=get_hostlist(et,'gen') # read conf, could be empty
	print 'genlist:'
	print genlist,len(genlist)

	memlist=get_hostlist(et,'mem')
	print 'memlist:'
	print memlist,len(memlist)

	sqllist=get_hostlist(et,'sql')
	print 'sqllist:'
	print sqllist,len(sqllist)

	coorlist=get_hostlist(et,'coor')
	print 'coorlist:',coorlist

	haplist=get_hostlist(et,'hap')
	print 'haplist:',haplist

	allhosts=get_allhosts(et)
	print 'all hosts:',allhosts

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
	print myip+' is : '+tname+', role: '+nametag

#  don't sleep before conf done, may rewrite set_params from coor.
# RPC port
	masterport=int(readconf.get_conf(et,'masterport'))
# init timeout,
	serverruntime=int(readconf.get_conf(et,'testtime')) # must recv cmd within sec
# rewrite log: newMonitorFile?.cpuMeasIntv 0.5 .monitorSampleTime 5
	rewrite=int(readconf.get_conf(et,'newMonitorFile'))
	sampleIntv=float(readconf.get_conf(et,'monitorSampleTime'))
	measureIntv=float(readconf.get_conf(et,'cpuMeasIntv'))
	rqps=int(readconf.get_conf(et,'rqps'))
	usecache=readconf.get_conf(et,'usecache')
	usemysql=readconf.get_conf(et,'usemysql')
	writefile=readconf.get_conf(et,'writefile')
	sampletime=readconf.get_conf(et,'sampletime')
	hitratio=readconf.get_conf(et,'hitratio')
	newMonitorFile=readconf.get_conf(et,'newMonitorFile') # if rewrite monitor.log
	cpuMeasIntv=readconf.get_conf(et,'cpuMeasIntv')  # measure cpu block intv time
	monitorSampleTime=readconf.get_conf(et,'monitorSampleTime') # sample every 5 second
	gentime=int(readconf.get_conf(et,'testtime'))
	rpctimeout=int(readconf.get_conf(et,'servertimeout'))
	skipservicecheck=int(readconf.get_conf(et,'skipservicecheck'))
	skipwarmup=int(readconf.get_conf(et,'skipwarmup'))
	cleanlog=int(readconf.get_conf(et,'cleanlog'))
	cleanres=int(readconf.get_conf(et,'cleanres'))
	delaytime=1.0/rqps

# START RPC SERVER
	tgen = threading.Thread(target=rpc_server_thread)
	tgen.setDaemon(True) # when main exit, this thread also terminate
	tgen.start()
	print tname+"  RPC server thread running... "+nametag

# cleaning
	if cleanlog>=1:
		cmd='ssh -tt '+host2userip[tname]+' sh '+host2dir(tname)+'/clean_log.sh'
		print '\nclean cmd: '+cmd
		subprocess.call(cmd.split())
		try:
			fn=glob.glob(host2dir(tname)+'/*.log')
			for rmf in fn:
				os.remove(rmf)
		except:
			print '*.log already removed !'
	if cleanres>=1:
		cmd='ssh -tt '+host2userip[tname]+' sh '+host2dir(tname)+'/clean_res.sh'
		print '\nclean cmd: '+cmd
		subprocess.call(cmd.split())
		
	# if nametag=='':
	# 	if tname in masterlist:
	# 		nametag='master'
	# 	elif tname in slavelist:
	# 		nametag='slave'
	# 	elif tname in coorlist:
	# 		nametag='coor'
	# 	else:
	# 		print 'what is my role ???'
	# 		sys.exit(1)
	# 	print 'my role: '+nametag
	if nametag=='':
		if tname in weblist:
			nametag='web'
		elif tname in genlist:
			nametag='gen'
		elif tname in memlist:
			nametag='mem'
		elif tname in sqllist:
			nametag='sql'
		elif tname in coorlist:
			nametag='coor'
		elif tname in haplist:
			nametag='hap'
		else:
			print 'what is my role ???'
		print 'my role: '+nametag
		
# SET LOG, not for storing results.
	logging.basicConfig(level=logging.INFO)
	logger = logging.getLogger(__name__)
	handler = logging.FileHandler(host2dir(tname)+'/'+nametag+'-'+tname+'.log')
	handler.setLevel(logging.INFO)
	formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
	handler.setFormatter(formatter)
	logger.addHandler(handler)

# restart/stop services:
	# if nametag in ['master','slave']:
	# 	#cmd='ssh -tt '+host2userip[tname]+' sh '+host2dir(tname)+'/restart_service.sh '+ser
	# 	cmd='disco restart'
	# 	print '\n cmd: '+cmd
	# 	subprocess.call(cmd.split())
# restart/stop services:
	if not nametag in ['web']:
		cmd='ssh -tt '+host2userip[tname]+' sh '+host2dir(tname)+'/stop_service.sh lighttpd'
		print '\n cmd: '+cmd
		subprocess.call(cmd.split())
	if nametag in ['web','mem','hap']:
		servlist=host2servicedict(tname)[nametag]
		for ser in servlist:
			cmd='ssh -tt '+host2userip[tname]+' sh '+host2dir(tname)+'/restart_service.sh '+ser
			print '\n cmd: '+cmd
			subprocess.call(cmd.split())
# set net param:
	if nametag in ['web','gen','hap']:
		cmd='ssh -tt '+host2userip[tname]+' sh '+host2dir(tname)+'/netparams_mild.sh'
		print '\n cmd: '+cmd
		subprocess.call(cmd.split())

# say I'm ready:
	print nametag+' telling coor ready '+tname
	proxy = xmlrpclib.ServerProxy('http://'+host2ip[coorlist[0]]+':'+str(masterport))
	proxy.from_worker_to_coor({'ready':tname})

# WAIT FOR COOR CMD	
	starttime=time.time()
	endtime=starttime+rpctimeout
	print tname+"  waiting for coor run cmd "
	while time.time()<endtime and not runflag:
		pass

	if not runflag:
		print tname+"  wait timeout... terminate "
		logger.info(nametag+" wait timeout... terminate "+tname)
		sys.exit(0)

# set MONITOR
	t = threading.Thread(target=th_monitor_run,args=(rewrite,sampleIntv,measureIntv))
	t.setDaemon(True)
	t.start()
# power thread
	if tname.startswith('t'):
		t = threading.Thread(target=th_get_power,args=(tname,))
		t.setDaemon(True)
		t.start()
####### do init work here, like putting files in place.(not service restart or clean)


			TODO


# AFTER INIT WORK DONE, tell coor
# initial sync. coor doesn't have my synclock yet, coor is waiting.
	workstart=0
	proxy=xmlrpclib.ServerProxy('http://'+host2ip[coorlist[0]]+':'+str(masterport))
	proxy.from_worker_to_coor({'who':tname,'synclock':synclock})
	# wait for coor step cmd,
	while workstart==0 and runflag: pass

# START WORKING
	try:
		if nametag!='': # I'm role?
			starttime=time.time()
			endtime=starttime+int(readconf.get_conf(et,'testtime'))
			while time.time()<endtime and runflag:

				jobdone=0
				haveexception=False
				firstjob=False
				secondjob=False
				tellcoor=False

				while 0== jobdone and runflag:
					try: # exception zone
						if (not haveexception) or (haveexception and not firstjob):
###### do first job here:
							print 'worker %s job 1'%tname


							'''TODO'''



###### after job success
						firstjob=True
## second job?

	###### after job, tell coor sync
						workstart=0
						print '%s tell coor sync %d'%(tname,synclock)
						resdict={'who':tname,'synclock':synclock}
						resdict['workererror']=''
##### tell coor results here: resdict['?']=''
						

						'''TODO'''



						if (not haveexception) or (haveexception and not tellcoor):
							proxy=xmlrpclib.ServerProxy('http://'+host2ip[coorlist[0]]+':'+str(masterport))
							proxy.from_worker_to_coor(resdict)
							# wait for coor step cmd
							while workstart==0 and runflag: pass
						tellcoor=True
	###### after recv coor instruction, do something, config next job here:


						'''TODO'''

						# jobdone? musttellcoor to finish?
						#jobdone=1
						#musttellcoor=1

	#### ######## ######### next round, job done determined by coor, not here.
						haveexception=False
						firstjob=False
						secondjob=False
						tellcoor=False
					except Exception, error:
						logger.info(str(traceback.format_exc()))
						proxy=xmlrpclib.ServerProxy('http://'+host2ip[coorlist[0]]+':'+str(masterport))
						msg=tname+' ERROR %s %s, survived.'%(str(firstjob),str(tellcoor))
						proxy.from_worker_to_coor({'workererror':msg})
						haveexception=True
		#elif nametag=='slave':
		else: # not major nametag
			print nametag+'\n\nnothing\n'
			endtime=time.time()+int(readconf.get_conf(et,'testtime'))	
			while time.time()<endtime and runflag:
				workstart=0
				proxy=xmlrpclib.ServerProxy('http://'+host2ip[coorlist[0]]+':'+str(masterport))
				proxy.from_worker_to_coor({'who':tname,'synclock':synclock})
				# wait for coor step cmd,
				while workstart==0 and runflag: pass
				time.sleep(3)
	except Exception, error:
		logger.info("Exception! %s %s died."%(nametag,tname))	
		logger.info(str(traceback.format_exc()))
		if runflag:
			proxy=xmlrpclib.ServerProxy('http://'+host2ip[coorlist[0]]+':'+str(masterport))
			proxy.from_worker_to_coor({'workererror':tname+' ERROR die! '})

# tell coor finished	
	if runflag or musttellcoor: # if not reset by coor
		proxy = xmlrpclib.ServerProxy('http://'+host2ip[coorlist[0]]+':'+str(masterport))
		proxy.from_worker_to_coor({'finish':tname})
# FINISH 	
	logger.info(ipsufix+" out of while FINISH. "+tname)	
	runflag=0
	print tname+" stopped main loop \n\n"
	main_thread = threading.currentThread()
	for t in threading.enumerate():
	    if t is main_thread:
	        continue
	    print tname+' '+(nametag+': joining %s' % t.getName())
	    t.join(0.1) # timeout after 1s, if still isAlive(), then rely on daemon kill

	print tname+" __main__ exit. "+nametag
