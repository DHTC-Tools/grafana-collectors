#!/bin/env python

# Gather pilot informaton from the CONNECT Panda Queues

# Import some standard python utilities
import sys, time
import classad, htcondor 



# Base path under which to store the gathered data
graphBase = "collect.connect.atlas.pilots"

# The Collectors which are running AutoPyFactory for which we are to Collect the data
collectorADs = [
	"rccf-gk.mwt2.org:11010?sock=collector"
]

# The queues which are running pilots on these AutoPyFactories
queueADs = [
	[ 'CONNECT'              ], # Production SCORE
	[ 'CONNECT_CLOUD'        ], # Production SCORE
	[ 'CONNECT_MCORE'        ], # Production MCORE
	[ 'CONNECT_PILE'         ], # Production PILE
	[ 'CONNECT_PILE_MCORE'   ], # Production PILE MCORE
	[ 'ANALY_CONNECT'        ], # Analysis SCORE
	[ 'ANALY_CONNECT_SHORT'  ], # Analysis SCORE
]


# Start with a blank set of metrics
collectMetrics = {}

# This will add a metric or increment it
def addMetric(metricName, count=1) :

  # Build a metric name using the current Collector name
  metric = "%s.%s" % (scheddCollect, metricName)

  if (metric in collectMetrics) :
    collectMetrics[metric] += count
  else :
    collectMetrics[metric]  = count


# Search all Collectors listed
for collectorAD in collectorADs[:] :

  # The full name of the collector
  collectorName    = collectorAD

  # Get the Collector information
  collectorClassAD = htcondor.Collector(collectorName)

  try :
    # Get a list of all Schedds associated with this Collector
    scheddADs = collectorClassAD.locateAll(htcondor.DaemonTypes.Schedd)
  except IOError, e :
    print >>sys.stderr, '%s Unable to contact collector to fetch schedds (collector: %s): %s' % (time.strftime("%Y-%m-%d %H:%M:%S"), collectorName, str(e))
    continue

  # Replace any . with _ in the Collector Name
  scheddCollector = '_'.join(collectorName.split('.'))

  # Find the jobs from each Schedd
  for scheddAD in scheddADs[:] :

    # The full name of this schedd
    scheddName = scheddAD['Name']

    # Replace any . with _ in the Schedd Name
    scheddCollect = '_'.join(scheddName.split('.'))


    # Get all the ClassADs for this Schedd associated with this Collector
    try :
      scheddClassADs = collectorClassAD.locate(htcondor.DaemonTypes.Schedd, scheddName)
      scheddClassAD  = htcondor.Schedd(scheddClassADs)
    except IOError, e :
      print >>sys.stderr, '%s Unable to contact schedd to fetch schedd classADs (schedd: %s) (collector: %s): %s' % (time.strftime("%Y-%m-%d %H:%M:%S"), scheddName, collectorName, str(e))
      continue



    # Loop over each Queue
    for queueAD in queueADs[:] :

      # Extract the queue name
      queueName = queueAD[0]


      # Running gathers lots of information
      try :
        jobADs = scheddClassAD.query('MATCH_APF_QUEUE=="%s" && jobstatus==2' % queueName)
      except IOError, e :
        print >>sys.stderr, '%s Unable to contact schedd to fetch job classADs (schedd: %s) (collector: %s): %s' % (time.strftime("%Y-%m-%d %H:%M:%S"), scheddName, collectorName, str(e))
        continue


      # Loop over each job that is running
      for jobAD in jobADs[:] : 

        # Get the number of cores used this job
        try :
          jobCores = jobAD['MachineAttrCpus0']
        except :
          jobCores = 1

        # Get the Pool the job is running in replacing any . with _
        jobRemotePool = '_'.join(jobAD['RemotePool'].split('.'))

        addMetric( "%s.Running"    %  queueName )
        addMetric( "%s.%s.Running" % (queueName, jobRemotePool) )
        addMetric( "%s.Cores"      %  queueName                 , jobCores)
        addMetric( "%s.%s.Cores"   % (queueName, jobRemotePool) , jobCores)



#     # Unexpanded
#     try :
#       jobADs = scheddClassAD.query('MATCH_APF_QUEUE=="%s" && jobstatus==0' % queueName)
#       for jobAD in jobADs[:] : addMetric( "%s.Unexpanded"  % queueName )
#     except IOError, e :
#       print >>sys.stderr, '%s Unable to contact schedd to fetch jobADs (schedd: %s) (collector: %s): %s' % (time.strftime("%Y-%m-%d %H:%M:%S"), scheddName, collectorName, str(e))
#       continue

      # Idle jobs 
      try :
        jobADs = scheddClassAD.query('MATCH_APF_QUEUE=="%s" && jobstatus==1' % queueName)
        for jobAD in jobADs[:]  : addMetric( "%s.Idle"       % queueName )
      except IOError, e :
        print >>sys.stderr, '%s Unable to contact schedd to fetch jobADs (schedd: %s) (collector: %s): %s' % (time.strftime("%Y-%m-%d %H:%M:%S"), scheddName, collectorName, str(e))
        continue

#     # Removed
#     try :
#       jobADs = scheddClassAD.query('MATCH_APF_QUEUE=="%s" && jobstatus==3' % queueName)
#       for jobAD in jobADs[:] : addMetric( "%s.Removed"     % queueName )
#     except IOError, e :
#       print >>sys.stderr, '%s Unable to contact schedd to fetch jobADs (schedd: %s) (collector: %s): %s' % (time.strftime("%Y-%m-%d %H:%M:%S"), scheddName, collectorName, str(e))
#       continue

#     # Completed
#     try :
#       jobADs = scheddClassAD.query('MATCH_APF_QUEUE=="%s" && jobstatus==4' % queueName)
#       for jobAD in jobADs[:] : addMetric( "%s.Completed"   % queueName )
#     except IOError, e :
#       print >>sys.stderr, '%s Unable to contact schedd to fetch jobADs (schedd: %s) (collector: %s): %s' % (time.strftime("%Y-%m-%d %H:%M:%S"), scheddName, collectorName, str(e))
#       continue

#     # Held
#     try :
#       jobADs = scheddClassAD.query('MATCH_APF_QUEUE=="%s" && jobstatus==5' % queueName)
#       for jobAD in jobADs[:] : addMetric( "%s.Held"        % queueName )
#     except IOError, e :
#       print >>sys.stderr, '%s Unable to contact schedd to fetch jobADs (schedd: %s) (collector: %s): %s' % (time.strftime("%Y-%m-%d %H:%M:%S"), scheddName, collectorName, str(e))
#       continue

#     # Submission error
#     try :
#       jobADs = scheddClassAD.query('MATCH_APF_QUEUE=="%s" && jobstatus==6' % queueName)
#       for jobAD in jobADs[:] : addMetric( "%s.Error"       % queueName )
#     except IOError, e :
#       print >>sys.stderr, '%s Unable to contact schedd to fetch jobADs (schedd: %s) (collector: %s): %s' % (time.strftime("%Y-%m-%d %H:%M:%S"), scheddName, collectorName, str(e))
#       continue




# Get the current time
timestamp = int(time.time())

# Loop over each metric and display its total and the current time
for metricName in collectMetrics :
  metricCount = collectMetrics[metricName]
  print "%s.%s %s %s" % (graphBase, metricName, metricCount, timestamp)
