#!/usr/bin/python

import sys
import requests
import argparse
import configparser
import json

parser = argparse.ArgumentParser(description='Check the status of Rancher agents and their containers.')
parser.add_argument('--config-file', dest='configfile', required=True,
		    help='Path to config file (INI format).')
parser.add_argument('--config-section', dest='section', required=True,
		    help='Section in config file to use.')
args = parser.parse_args()

configfile=args.configfile
conf=configparser.ConfigParser()
conf.read(configfile)
#print (conf.sections())

# include the port if needed
urlbase=conf[args.section]['rancher_url']
# would be better to use envname and query the api to find envid
# but the envid should seldom if ever change so getting on cmdline ok
envid=conf[args.section]['rancher_envid']
envname=conf[args.section]['rancher_envname']
stackname=conf[args.section]['rancher_stackname']
username=conf[args.section]['rancher_accesskey']
password=conf[args.section]['rancher_secretkey']

# look for these services (remaining cmdline args)
#monitoredServices = sys.argv[7:]
#monitoredServices = conf.get(args.section,'service_list')
monitoredServices = json.loads(conf.get(args.section,'service_list'))
print (monitoredServices)

session=requests.Session()
hostsReq=session.get(urlbase+'/v2-beta/projects/' + envid + '/hosts/', auth=(username,password))
hostData=hostsReq.json()['data']

# monitor an agent
for host in hostData:
	state=3
	stateText='UNKNOWN'

	if (host['state'] != 'active'):
		state=2
		stateText='CRITICAL'
	if (host['state'] == 'active'):
		state=0
		stateText='OK'

	instanceReq=session.get(host['links']['instances'],auth=(username,password))
	instanceData=instanceReq.json()['data']
#	print len(instanceData)
#	for instance in instanceData:
#		print instance
	print (str(state) + ' rancher_agent_' + host['hostname'] + ' - ' + stateText + ' host ' + host['hostname'] + ' running containers: ' + str(len(instanceData)))

# to do: monitor services inside a stack
stackReq=session.get(urlbase+'/v2-beta/projects/' + envid + '/stacks/', auth=(username,password))
stackData=stackReq.json()['data']

# assume there's only one
try:
	stackId = [i for i,j in enumerate(stackData) if j['name'] == stackname][0]
except:
	# assume no stack data; this is bad and need better handling
	sys.exit(0)

for serviceId in stackData[stackId]['serviceIds']:
#	print serviceId
# in that stack, look through serviceIds for named services in /v2-beta/projects/envid/services/serviceId
	serviceReq=session.get(urlbase+'/v2-beta/projects/' + envid + '/services/' + serviceId, auth=(username,password))
	svc=serviceReq.json()
	if svc['name'] in monitoredServices:
		serviceState = 3
		serviceStateTxt = 'UNKNOWN'

		if svc['healthState'] == 'healthy':
			serviceState = 0
			serviceStateTxt = 'OK'
		if svc['healthState'] == 'unhealthy':
			serviceState = 2
			serviceStateTxt = 'CRITICAL'

		print (str(serviceState) + ' ' + envname + '_' + stackname + '_' + svc['name'] + ' - ' + serviceStateTxt + ' running instances: ' + str(svc['currentScale']))
#	    print svc['healthState']


# in each service find the last logs?  may be hard, need websocket

