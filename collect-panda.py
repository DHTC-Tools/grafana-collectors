#!/usr/bin/env python

# Gather job information from the CONNECT Panda Queues

import string, time, pycurl
from StringIO import StringIO

# list of computing elements for which to query
computingelements = []

# Base path under which to store the gathered data
graphBase = "collect.connect.atlas.panda"

# The Collectors which are running AutoPyFactory for which we are to Collect the data
collectorADs = [
    "rccf-gk.mwt2.org:11010?sock=collector"
]

# The queues which are running pilots on these AutoPyFactories
queueADs = [
     [ 'MWT2_SL6'            ], # Production SCORE
     [ 'MWT2_MCORE'          ], # Production MCORE
     [ 'ANALY_MWT2_SL6'      ], # Analysis SCORE
     [ 'CONNECT'             ], # Production SCORE
     [ 'CONNECT_CLOUD'       ], # Production SCORE
     [ 'CONNECT_MCORE'       ], # Production MCORE
     [ 'CONNECT_PILE'        ], # Production PILE
     [ 'CONNECT_PILE_MCORE'  ], # Production PILE MCORE
     [ 'ANALY_CONNECT'       ], # Analysis SCORE
     [ 'ANALY_CONNECT_SHORT' ], # Analysis SCORE
]



# Start with a blank set of metrics
collectMetrics = {}

# This will add a metric or increment it
def addMetric(metricName, count=1) :

  # Build a metric name using the queue name
  metric = "%s.%s" % (queueName, metricName)

  if (metric in collectMetrics) :
    collectMetrics[metric] += count
  else :
    collectMetrics[metric]  = count





# Loop over each Queue
for queueAD in queueADs[:] :

  # Extract the queue name and number of cores per job
  queueName = queueAD[0]

  # Get json output from http://bigpanda.cern.ch
  curlBuffer = StringIO()
  curlCMD = pycurl.Curl()
  curlCMD.setopt(curlCMD.URL,           "http://bigpanda.cern.ch/jobs/?hours=1&limit=100000&fields=pandaid,modificationhost,jobstatus&computingsite=%s" % queueName)
# curlCMD.setopt(curlCMD.URL,           "http://bigpanda.cern.ch/jobs/?hours=1&limit=100000&fields=pandaid,modificationhost,jobstatus&computingelement=%s" % queueName)
  curlCMD.setopt(curlCMD.WRITEFUNCTION, curlBuffer.write)
  curlCMD.setopt(curlCMD.HTTPHEADER,    ["Accept: application/json"])
  curlCMD.setopt(curlCMD.HTTPHEADER,    ["Content-Type: application/json"])
  curlCMD.perform()
  jobADs = curlBuffer.getvalue().split("}, {")
  curlBuffer.close()
  curlCMD.close()


  # Start with a blank set of job metrics for this queue
  jobMetrics = {}

  # Initialize the Metric summation for this Panda Queue
  jobMetrics['METRICS'] = { \
    'pending'     : 0, \
    'defined'     : 0, \
    'waiting'     : 0, \
    'assigned'    : 0, \
    'throttled   ': 0, \
    'activated'   : 0, \
    'sent'        : 0, \
    'starting'    : 0, \
    'running'     : 0, \
    'holding'     : 0, \
    'merging'     : 0, \
    'transferring': 0, \
    'finished'    : 0, \
    'failed'      : 0, \
    'cancelled'   : 0, \
    'unknown'     : 0, \
  }

  # Process json output for each job
  for jobAD in jobADs[:] :

    # Reformat the job into a dict
    job = jobAD.split()


    # Short string means the job is empty
    if (len(job) == 1) : continue

    # Extract the host on which the job is running
    try :
      jobHost = job[job.index('"modificationhost":')+1][1:-1].split("@")[-1]

    # If there is no host, then this must be a Queue only metric
    except :
      jobHost = 'QUEUE'


    # Set a job Site based on the node name the job is running in
    if   ('QUEUE' == jobHost) :
      jobSite = 'QUEUE'
    elif ('aipanda' in jobHost) :
      jobSite = 'PANDA'
    elif ('uct2-c' in jobHost) :
      jobSite = 'uchicago'
    elif ('iut2-c' in jobHost) :
      jobSite = 'indiana'
    elif ('golub' in jobHost) | ('taub' in jobHost):
      jobSite = 'illinois'
    elif ('.karst.uits.iu.edu' in jobHost) :
      jobSite = 'karst'
    elif ('.atlas.fresnostate.edu' in jobHost) :
      jobSite = 'fresnostate'
    elif ('.rc.fas.harvard.edu' in jobHost) | ('airoldi' in jobHost):
      jobSite = 'odyssey'
    elif ('midway' in jobHost) :
      jobSite = 'midway'
    elif ('.stampede.tacc.utexas.edu' in jobHost) :
      jobSite = 'stampede'
    elif ('.rodeo.tacc.utexas.edu' in jobHost) :
      jobSite = 'utexas'
    else :
      jobSite = 'UNKNOWN'


    # Extract the job status key
    try:
      jobStatus = job[job.index('"jobstatus":')+1][1:-2].lower() 
    except :
      jobStatus = 'unknown'


    # If this is a new Site, intialize the metrics
    if (jobSite not in jobMetrics) : 
       jobMetrics[jobSite] = { \
         'pending'     : 0, \
         'defined'     : 0, \
         'waiting'     : 0, \
         'assigned'    : 0, \
         'throttled   ': 0, \
         'activated'   : 0, \
         'sent'        : 0, \
         'starting'    : 0, \
         'running'     : 0, \
         'holding'     : 0, \
         'merging'     : 0, \
         'transferring': 0, \
         'finished'    : 0, \
         'failed'      : 0, \
         'cancelled'   : 0, \
         'unknown'     : 0, \
        }


    # Increment appropriate job status
    jobMetrics[jobSite][jobStatus] += 1

    # Increment the Queue metrics for this job
    jobMetrics['METRICS'][jobStatus] += 1



  # Add each of these metrics to the collect metrics list
  for jobSite in jobMetrics :
    for jobStatus in jobMetrics[jobSite] :
        n = int(jobMetrics[jobSite][jobStatus])
        addMetric( "%s.%s"  % (jobSite, jobStatus) , n )



# Get the current time
timestamp = int(time.time())

# Loop over each metric and display its total and the current time
for metricName in collectMetrics :
  metricCount = collectMetrics[metricName]
  print "%s.%s %s %s" % (graphBase, metricName, metricCount, timestamp)
