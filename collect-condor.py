#!/bin/env python

# Gather information on the Condor slots from all Collectors

# Import some standard python utilities
import sys, time
import classad, htcondor 

# Base path for underwhich we store the gathered data
graphBase = "collect.condor"
#graphBase = "ddlesny.condor"

# The Collectors for which we are to Collect the data
collectorADs ={ 
     'osg-condor.mwt2.org'                            : 'mwt2' , # MWT2 UChicago NAT
     'uct2-condor.mwt2.org'                           : 'mwt2' , # MWT2 UChicago
     'iut2-condor.mwt2.org'                           : 'mwt2' , # MWT2 Indiana
     'mwt2-condor.campuscluster.illinois.edu'         : 'mwt2' , # MWT2 Illinois
     'rccf-atlas.ci-connect.net:11017?sock=collector' : 'rccf' , # UTexas
     'rccf-atlas.ci-connect.net:11018?sock=collector' : 'rccf' , # FresnoState
     'rccf-atlas.ci-connect.net:11019?sock=collector' : 'rccf' , # Stampede
     'rccf-atlas.ci-connect.net:11020?sock=collector' : 'rccf' , # Midway
     'rccf-atlas.ci-connect.net:11021?sock=collector' : 'rccf' , # Stampede MCORE
     'rccf-atlas.ci-connect.net:11022?sock=collector' : 'rccf' , # Midway MCORE
     'rccf-atlas.ci-connect.net:11023?sock=collector' : 'rccf' , # ICC Taub
     'rccf-atlas.ci-connect.net:11024?sock=collector' : 'rccf' , # ICC Golub
     'rccf-atlas.ci-connect.net:11025?sock=collector' : 'rccf' , # ICC MCORE
     'rccf-atlas.ci-connect.net:11026?sock=collector' : 'rccf' , # ICC HEPT3
     'rccf-atlas.ci-connect.net:11027?sock=collector' : 'rccf' , # Odyssey
     'rccf-atlas.ci-connect.net:11028?sock=collector' : 'rccf' , # Odyssey Short
     'rccf-atlas.ci-connect.net:11029?sock=collector' : 'rccf' , # Karst
     'rccf-atlas.ci-connect.net:11030?sock=collector' : 'rccf' , # Karst MCORE
     'rccf-atlas.ci-connect.net:11031?sock=collector' : 'rccf' , # Karst Preempt
     'rccf-atlas.ci-connect.net:11032?sock=collector' : 'rccf' , # ICC Golub20
     'rccf-atlas.ci-connect.net:11120?sock=collector' : 'rccf' , # AtlasConnect MWT2
     'rccf-atlas.ci-connect.net:11121?sock=collector' : 'rccf' , # AtlasConnect AGLT2
     'uct2-bosco.uchicago.edu:11010?sock=collector'   : 'rccf' , # Tier3Connect Illinois
     'uct2-bosco.uchicago.edu:11013?sock=collector'   : 'rccf' , # Tier3Connect UChicago
     'uct2-bosco.uchicago.edu:11015?sock=collector'   : 'rccf' , # Tier3Connect Indiana
     'uct2-bosco.uchicago.edu:11017?sock=collector'   : 'rccf' , # Tier3Connect UTexas
     'uct2-bosco.uchicago.edu:11126?sock=collector'   : 'rccf' , # MWT2 UChicago
     'uct2-bosco.uchicago.edu:11127?sock=collector'   : 'rccf' , # MWT2 Indiana
     'uct2-bosco.uchicago.edu:11128?sock=collector'   : 'rccf' , # MWT2 Illinois
     'uct2-bosco.uchicago.edu:11129?sock=collector'   : 'rccf' , # MWT2 UChicago MCORE
     'uct2-bosco.uchicago.edu:11130?sock=collector'   : 'rccf' , # MWT2 Indiana  MCORE
     'uct2-bosco.uchicago.edu:11131?sock=collector'   : 'rccf' , # MWT2 Illinois MCORE
     'uct2-bosco.uchicago.edu:11132?sock=collector'   : 'rccf' , # MWT2 UChicago Pile
     'uct2-bosco.uchicago.edu:11133?sock=collector'   : 'rccf' , # MWT2 Indiana  Pile
     'uct2-bosco.uchicago.edu:11134?sock=collector'   : 'rccf' , # MWT2 Illinois Pile
     'uct2-bosco.uchicago.edu:11135?sock=collector'   : 'rccf' , # MWT2 UChicago Pile MCORE
     'uct2-bosco.uchicago.edu:11136?sock=collector'   : 'rccf' , # MWT2 Indiana  Pile MCORE
     'uct2-bosco.uchicago.edu:11137?sock=collector'   : 'rccf' , # MWT2 Illinois Pile MCORE
}

# APF queue name to AccountingGroup map
apfQueueACCT = {
     'MWT2_SL6'            : 'group_atlas_prod_score',  # Production SCORE
     'MWT2_MCORE'          : 'group_atlas_prod_mcore',  # Production MCORE
     'ANALY_MWT2_SL6'      : 'group_atlas_analy_score', # Analysis SCORE
     'CONNECT'             : 'group_atlas_prod_score',  # Production SCORE
     'CONNECT_CLOUD'       : 'group_atlas_prod_score',  # Production SCORE
     'CONNECT_MCORE'       : 'group_atlas_prod_mcore',  # Production MCORE
     'CONNECT_PILE'        : 'group_atlas_prod_score',  # Production PILE
     'CONNECT_PILE_MCORE'  : 'group_atlas_prod_mcore',  # Production PILE MCORE
     'ANALY_CONNECT'       : 'group_atlas_analy_score', # Analysis SCORE
     'ANALY_CONNECT_SHORT' : 'group_atlas_analy_score', # Analysis SCORE
}

################################################################################

# Start with a blank set of Collect metrics
metricCollectDICT = {}

# This will add or increment a metric in the Collect DICT
# The full path (site, pool) is prepended to the key

def addMetricCollect( metricCollectorName, metricCollectorSite, metricKey, metricCount=1 ) :

  # Build a metric Key using the site of the collector and the slots collector
  metricKey = "%s.%s.%s" % (metricCollectorSite.replace('.','_'), metricCollectorName.replace('.','_'), metricKey)

  if (metricKey in metricCollectDICT) :
    metricCollectDICT[metricKey] += metricCount
  else :
    metricCollectDICT[metricKey]  = metricCount

 
# This will add or increment a metric in the Collect DICT
# No modification is done to the metricKey, it is used as is

def addMetricCollectKey( metricKey, metricCount=1 ) :

  if (metricKey in metricCollectDICT) :
    metricCollectDICT[metricKey] += metricCount
  else :
    metricCollectDICT[metricKey]  = metricCount

################################################################################

# Set a metric in the given metric dictionary

def setMetricDICT( metricDICT, metricID, metricKey, metricValue='') :

  # If the given ID does not exist in the given dictionary add it
  # Otherwise, set the value in the given key

  if ( metricID in metricDICT ) :
    metricDICT[metricID][metricKey] = metricValue
  else :
    metricDICT[metricID] = { metricKey : metricValue }


# Add or increment a metric in the given dictionary

def addMetricDICT( metricDICT, metricID, metricKey, metricCount=0) :

  # If the given ID does not exist in the given dictionary add it
  # Otherwise, increment the current value by the given Count

  if ( metricID in metricDICT ) :
    if ( metricKey in metricDICT[metricID] ) :
      metricDICT[metricID][metricKey] += int(metricCount)
    else :
      metricDICT[metricID][metricKey]  = int(metricCount)
  else :
    metricDICT[metricID] = { metricKey : int(metricCount) }


################################################################################

# Initialize the job metric dictionary and key values
metricJobDICT          = {}


# Set a metric in the Job DICT
# setMetricJobDICT( metricGlobalJobID, 'Owner', 'usatlas1' )
# setMetricJobDICT( metricGlobalJobID, 'CPU', 123456789 )

def setMetricJobDICT( metricGlobalJobID, metricKey, metricValue='') :
  setMetricDICT( metricJobDICT, metricGlobalJobID, metricKey, metricValue )


# Add or increment a metric in the Job DICT
# addMetricJobDICT( metricGlobalJobID, 'WallClockTime', 1234567890 )

def addMetricJobDICT( metricGlobalJobID, metricKey, metricCount=1) :
  addMetricDICT( metricJobDICT, metricGlobalJobID, metricKey, metricCount )


################################################################################

# Initialize the schedd metric dictionary
metricScheddDICT = {}


# Set a metric in the Schedd DICT
# setMetricScheddDICT( metricSchedd, 'Count', 1 )
# setMetricScheddDICT( metricSchedd, 'Host', 'hostname')

def setMetricScheddDICT( metricSchedd, metricKey, metricValue='') :
  setMetricDICT( metricScheddDICT, metricSchedd, metricKey, metricValue )


# Add or increment a metric in the Schedd DICT
# addMetricScheddDICT( metricSchedd, 'Count', 1)

def addMetricScheddDICT( metricSchedd, metricKey, metricCount=1 ) :
  addMetricDICT( metricScheddDICT, metricSchedd, metricKey, metricCount )


################################################################################

# Initialize the schedd metric dictionary
metricSlotDICT = {}


# Set a metric in the Slot DICT
# setMetricSlotDICT( metricSlot, 'Count', 1 )
# setMetricSlotDICT( metricSlot, 'Host', 'hostname')

def setMetricSlotDICT( metricSlot, metricKey, metricValue='') :
  setMetricDICT( metricSlotDICT, metricSlot, metricKey, metricValue )


# Add or increment a metric in the Schedd DICT
# addMetricSlotDICT( metricSlot, 'Count', 1)

def addMetricSlotDICT( metricSlot, metricKey, metricCount=1 ) :
  addMetricDICT( metricSlotDICT, metricSlot, metricKey, metricCount )


################################################################################

# Initialize the Efficiency metrics
metricPoolDICT  = {}
metricAcctDICT  = {}
metricHostDICT  = {}
metricOwnerDICT = {}


# Computes the efficiency for a given metric
# Efficiency accounts for the number of cores by using Core Wall Clock value

def effMetricDICT ( metricDICT ) :

  # Fetch all the information for each entry
  for metricName in metricDICT :
    jobCpuTime           = metricDICT[metricName]['CpuTime']
    jobCores             = metricDICT[metricName]['Cores']
    jobWallClockTime     = metricDICT[metricName]['WallClockTime']
    jobCoreWallClockTime = metricDICT[metricName]['CoreWallClockTime']


    # Compute the job efficency as percent from 0 to 100
    if (jobCoreWallClockTime == 0) : 
      jobEfficiency = 0
    else:
      jobEfficiency = int((float(jobCpuTime) / float(jobCoreWallClockTime)) * 100.0)

    if ( jobEfficiency > 100 ) :
      print >>sys.stderr, '%s Bogus job efficiency:%s   Key:%s   CPU:%s   Cores:%s   WallClockTime:%s   CoreWallClockTime:%s' % (time.strftime("%Y-%m-%d %H:%M:%S"), jobEfficiency, metricName, jobCpuTime, jobCores, jobWallClockTime, jobCoreWallClockTime)
      jobCpuTime           = 0
      jobWallClockTime     = 0
      jobCoreWallClockTime = 0
      jobEfficiency        = 0


    # Store each into the Collect Metric dict
    addMetricCollectKey( metricName + ".cputime"           , jobCpuTime           )
    addMetricCollectKey( metricName + ".cores"             , jobCores             )
    addMetricCollectKey( metricName + ".wallclocktime"     , jobWallClockTime     )
    addMetricCollectKey( metricName + ".corewallclocktime" , jobCoreWallClockTime )
    addMetricCollectKey( metricName + ".efficiency"        , jobEfficiency        )



################################################################################
# Pass 1 - Collect all slot information from the list of Collectors (Pools)
################################################################################


# Check each Collector one at a time
for collectorName in collectorADs :

  # Extract the collector site
  collectorSite = collectorADs[collectorName]

  # Get the Collector information
  collectorClassAD = htcondor.Collector(collectorName)

  # Fetch the ClassADs for every slot in this Collector
  try :
    slotClassADs = collectorClassAD.query(htcondor.AdTypes.Startd, "true", [ 'Name', 'Collector_Host_String', 'GlobalJobID', 'ClientMachine', 'JobID', 'State', 'Activity', 'SlotIsHealthy', 'SlotHealthReason', 'RemoteOwner', 'Cpus', 'AccountingGroup' ])
  except IOError, e :
    print >>sys.stderr, '%s Unable to contact collector to fetch slot classADs (collector: %s): %s' % (time.strftime("%Y-%m-%d %H:%M:%S"), collectorName, str(e))
    continue


  ## Fetch information on all slots in this Pool

  # Look at the ClassADs for each slot in this Pool
  for slotClassAD in slotClassADs[:] :

    # Fetch the Name of this slot
    try :
      slotName = slotClassAD['Name']
    except :
      slotName = ''

    # Fetch the Pool name for this slot
    try :
      slotPool = slotClassAD['Collector_Host_String']
    except :
      slotPool = ''

    # Fetch the GlobalJobID for any job running in this slot
    try :
      slotGlobalJobID = slotClassAD['GlobalJobID']
    except :
      slotGlobalJobID = ''

    # Fetch the ClientMachine for the job running in this slot
    try :
      slotClientMachine = slotClassAD['ClientMachine']
    except :
      slotClientMachine = ''

    # Fetch the JobID for the job running in this slot
    try :
      slotJobID = slotClassAD['JobID']
    except :
      slotJobID = ''

    # Get the current state of the slot (Claimed, Unclaimed, Owner, etc)
    try :
      slotState = slotClassAD['State']
    except:
      slotState = 'Owner'

    # Get the current activity of the slot (Busy, Idle, Suspended)
    try :
      slotActivity = slotClassAD['Activity']
    except :
      slotActivity = ''

    # Get the Health of the Slot catching any that do not have this ClassAD as yet
    try :
      slotIsHealthy = slotClassAD['SlotIsHealthy']
    except :
      slotIsHealthy = True
 
    # Get the Health Reason of the Slot catching any that do not have this ClassAD as yet
    try :
      slotHealthReason = slotClassAD['SlotHealthReason']
    except :
      slotHealthReason = True
 
    # Get the Owner of the slot removing the UID_DOMAIN and replacing '.' with '_'
    try :
      slotOwner = slotClassAD['RemoteOwner'].split('@')[0]
    except :
      slotOwner = ''

    # Get the AccountingGroup in use by the slot removing the User and Domain
    # AccountGroup is of the form 'group_atlas.prod.score.ruc.uchicago@osg-gk.mwt2.org'
    # Split on the @ to remove the domain "@osg-gk.mwt2.org"
    # Split on the last "."    to remove the user
    # Split on the last ".ruc" to remove the leading part of the user of the RUC username 
    try :
      slotAcct = slotClassAD['AccountingGroup'].split('@')[0].rsplit('.',1)[0].rsplit('.ruc',1)[0]
    except :
      slotAcct = ''

    # Get the number of CPUs assigned to this slot
    try :
      slotCpus = int(slotClassAD['Cpus'])
    except :
      slotCpus = 1


    # Initialize the reported state for slot and cpu usage
    stateSlot = 'Unknown'
    stateCpus = 'Unknown'

    if (slotIsHealthy == False) :
      stateSlot = 'NotHealthy'
      stateCpus = 'Idle'
    elif (slotState == 'Claimed') :
      if   (slotActivity == 'Busy') :
        stateSlot = 'Busy'
        stateCpus = 'Busy'
      elif (slotActivity == 'Retiring') :
        stateSlot = 'Retiring'
        stateCpus = 'Busy'
      elif (slotActivity == 'Idle') :
        stateSlot = 'Idle'
        stateCpus = 'Idle'
      elif (slotActivity == 'Suspended') :
        stateSlot = 'Busy'
        stateCpus = 'Busy'
    elif (slotState == 'Unclaimed') :
      stateSlot = 'Idle'
      stateCpus = 'Idle'
    elif (slotState == 'Owner') :
      stateSlot = 'Idle'
      stateCpus = 'Idle'
    elif (slotState == 'Matched') :
      stateSlot = 'Idle'
      stateCpus = 'Idle'
    elif (slotState == 'Preempting') :
      stateSlot = 'Retiring'
      stateCpus = 'Busy'


    # Save all the information we just gathered on the slot
    setMetricSlotDICT( slotName, 'GlobalJobID'      , slotGlobalJobID   )
    setMetricSlotDICT( slotName, 'ClientMachine'    , slotClientMachine )
    setMetricSlotDICT( slotName, 'JobID'            , slotJobID         )
    setMetricSlotDICT( slotName, 'State'            , slotState         )
    setMetricSlotDICT( slotName, 'Activity'         , slotActivity      )
    setMetricSlotDICT( slotName, 'Owner'            , slotOwner         )
    setMetricSlotDICT( slotName, 'AccountingGroup'  , slotAcct          )
    setMetricSlotDICT( slotName, 'SlotIsHealthy'    , slotIsHealthy     )
    setMetricSlotDICT( slotName, 'SlotHealthReason' , slotHealthReason  )
    setMetricSlotDICT( slotName, 'Cpus'             , slotCpus          )
    setMetricSlotDICT( slotName, 'Pool'             , slotPool          )
    setMetricSlotDICT( slotName, 'stateSlot'        , stateSlot         )
    setMetricSlotDICT( slotName, 'stateCpus'        , stateCpus         )
    setMetricSlotDICT( slotName, 'CollectorName'    , collectorName     )
    setMetricSlotDICT( slotName, 'CollectorSite'    , collectorSite     )


    # If we have a job running in this slot, save the information
    if ( slotGlobalJobID != '' ) :
      setMetricJobDICT( slotGlobalJobID, 'Validated'           , False             )
      setMetricJobDICT( slotGlobalJobID, 'ClientMachine'       , slotClientMachine )
      setMetricJobDICT( slotGlobalJobID, 'JobID'               , slotJobID         )
      setMetricJobDICT( slotGlobalJobID, 'SlotName'            , slotName          )
      setMetricJobDICT( slotGlobalJobID, 'SlotPool'            , slotPool          )
      setMetricJobDICT( slotGlobalJobID, 'SlotOwner'           , slotOwner         )
      setMetricJobDICT( slotGlobalJobID, 'SlotAccountingGroup' , slotAcct          )
      setMetricJobDICT( slotGlobalJobID, 'CollectorName'       , collectorName     )
      setMetricJobDICT( slotGlobalJobID, 'CollectorSite'       , collectorSite     )


################################################################################
# Pass 2 - Collect all information on the jobs runninng in the Pools slots from each schedd
################################################################################

# Check each Collector one at a time
for collectorName in collectorADs :

  # Extract the collector site
  collectorSite = collectorADs[collectorName]

  # Get the Collector information
  collectorClassAD = htcondor.Collector(collectorName)

  try :
    scheddADs = collectorClassAD.locateAll(htcondor.DaemonTypes.Schedd)
  except IOError, e :
    print >>sys.stderr, '%s Unable to contact collector to fetch schedds (collector: %s): %s' % (time.strftime("%Y-%m-%d %H:%M:%S"), collectorName, str(e))
    continue

  # Find the jobs from each Schedd asscociated with this Collector
  for scheddAD in scheddADs:

    # The full name of this schedd
    scheddName = scheddAD['Name']

    # If we have already processed this Schedd for jobs, move on
    if ( scheddName in metricScheddDICT ) : 
#     print >>sys.stderr, '%s Ignoring already processed Schedd: %s' % (time.strftime("%Y-%m-%d %H:%M:%S"), scheddName)
      continue

    # Determine which Pool this Schedd is bound to
    scheddPool = scheddAD['CollectorHost']


    # Get all the ClassADs for this Schedd
    try :
      scheddClassADs = collectorClassAD.locate(htcondor.DaemonTypes.Schedd, scheddName)
      scheddClassAD  = htcondor.Schedd(scheddClassADs)
    except IOError, e :
      print >>sys.stderr, '%s Unable to contact schedd to fetch classADs (schedd: %s) (collector: %s): %s' % (time.strftime("%Y-%m-%d %H:%M:%S"), scheddName, collectorName, str(e))
      continue

    # Find all running jobs on this Schedd
    try :
      jobADs = scheddClassAD.query('JobStatus==2')
    except IOError, e :
      print >>sys.stderr, '%s Unable to contact schedd to fetch job classADs (schedd: %s) (collector: %s): %s' % (time.strftime("%Y-%m-%d %H:%M:%S"), scheddName, collectorName, str(e))
      continue


    # Fetch classADs for every job running via this Schedd
    for jobAD in jobADs[:] :

      # Fetch the GlobalJobID for this job
      jobGlobalJobID = jobAD['GlobalJobID']

      # If the job is not found in the Pools we already scanned,
      # then this Schedd must have put it into another pool, ignore and move on
      if ( jobGlobalJobID not in metricJobDICT ) :
#       print >>sys.stderr, '%s Ignoring job found in Schedd %s (collector: %s), but not in searched Pools: %s' % (time.strftime("%Y-%m-%d %H:%M:%S"), scheddName, collectorName, jobGlobalJobID)
        continue

      # The job schedd is the one currently scanning for jobs
      jobSchedd = scheddName


      # Fetch the pool associated with this job
      # If there is none, see if this schedd is in the local pool
      try :
        jobPool = jobAD['RemotePool']
      except :
        jobPool = ''


      # Get the owner of the job
      try :
        jobOwner = jobAD['Owner']
      except :
        jobOwner = ''

      # Get the remote host on which the job is running
      # Remote any preceeding "slotN@" from the name
      try :
        jobHost = jobAD['RemoteHost']
        if ( '@' in jobHost ) : jobHost = jobHost.rsplit('@',1)[1]
      except :
        jobHost = ''



      # Fetch and cleanup the AccountingGroup
      # AccountingGroup is of the form 'group_atlas.prod.score.ruc.uchicago@osg-gk.mwt2.org'
      # Split on the @ to remove the domain "@osg-gk.mwt2.org"
      # Split on the last "."    to remove the user
      # Split on the last ".ruc" to remove the leading part of the user of the RUC username 
      try :
        jobAcct = jobAD['AccountingGroup'].split('@')[0].rsplit('.',1)[0].rsplit('.ruc',1)[0]
      except :
        jobAcct = ''


      # Get the APF Queue if available removing any trailing gatekeeper name (after first -)
      # Replace any "." to "_" for graphite as a dot is a separator
      try :
        jobQueue = jobAD['Match_APF_Queue'].split('-')[0]
      except :
        jobQueue = ''


      # If the job does not have an AccountingGroup, try to determine it from the Queue name
      if ( jobAcct == '' ) : 
        try :
          jobAcct = apfQueueACCT[jobQueue]
        except :
          if ( jobQueue == '') :
            jobAcct = 'group_NONE'
          else :
            jobAcct = "group_UNKNOWN-APF-QUEUE-%s" % jobQueue


      # Get the number of cores used by this job
      try :
        jobCores = int(jobAD['MachineAttrCpus0'])
      except :
        jobCores = 1


      # Get the CPU (User + Sys)
      try :
        jobCpuTime = int(jobAD['RemoteUserCpu']) + int(jobAD['RemoteSysCpu'])
      except :
        jobCpuTime = 0


      # Get the Wall clock time used by the job in the run state adjusted for the number of cores
      try :
        jobWallClockTime = int(time.time()) - int(jobAD['JobStartDate'])
#       jobWallClockTime = int(time.time()) - int(jobAD['EnteredCurrentStatus'])
#       jobWallClockTime = int(jobAD['RemoteWallClockTime'] - jobAD['CumulativeSuspensionTime'])
      except :
        jobWallClockTime = 0


      # Compute the Core Wall Clock Time
      jobCoreWallClockTime = jobWallClockTime * jobCores

      # Compute the jobs efficiency
      jobEfficiency = int((float(jobCpuTime) / float(jobCoreWallClockTime)) * 100.0)


      # Save all the job metrics we just gathered
      setMetricJobDICT( jobGlobalJobID, 'Validated'          , True                 )
      setMetricJobDICT( jobGlobalJobID, 'Schedd'             , jobSchedd            )
      setMetricJobDICT( jobGlobalJobID, 'Host'               , jobHost              )
      setMetricJobDICT( jobGlobalJobID, 'Queue'              , jobQueue             )
      setMetricJobDICT( jobGlobalJobID, 'Cores'              , jobCores             )
      setMetricJobDICT( jobGlobalJobID, 'CpuTime'            , jobCpuTime           )
      setMetricJobDICT( jobGlobalJobID, 'WallClockTime'      , jobWallClockTime     )
      setMetricJobDICT( jobGlobalJobID, 'CoreWallClockTime'  , jobCoreWallClockTime )
      setMetricJobDICT( jobGlobalJobID, 'Efficiency'         , jobEfficiency        )
      setMetricJobDICT( jobGlobalJobID, 'JobPool'            , jobPool              )
      setMetricJobDICT( jobGlobalJobID, 'JobOwner'           , jobOwner             )
      setMetricJobDICT( jobGlobalJobID, 'JobAccountingGroup' , jobAcct              )

      # Store a cummilative of the job metrics for this Schedd
      addMetricScheddDICT( scheddName,  'Running'            , 1                    )
      addMetricScheddDICT( scheddName,  'Cores'              , jobCores             )
      addMetricScheddDICT( scheddName,  'CpuTime'            , jobCpuTime           )
      addMetricScheddDICT( scheddName,  'WallClockTime'      , jobWallClockTime     )
      addMetricScheddDICT( scheddName,  'CoreWallClockTime'  , jobCoreWallClockTime )


################################################################################
# Pass 3 - Build the Slot Collect Metrics from the data we have collected
################################################################################

# Slot metrics
for slotName in metricSlotDICT :

  # Fetch the slot metrics we need
  collectorName = metricSlotDICT[slotName]['CollectorName']
  collectorSite = metricSlotDICT[slotName]['CollectorSite']
  slotAcct      = metricSlotDICT[slotName]['AccountingGroup']
  slotCpus      = metricSlotDICT[slotName]['Cpus']
  slotOwner     = metricSlotDICT[slotName]['Owner']
  stateSlot     = metricSlotDICT[slotName]['stateSlot']
  stateCpus     = metricSlotDICT[slotName]['stateCpus']

  # Replace '.' with '_' for graphite
  _slotOwner = slotOwner.replace('.','_')
  _slotAcct  = slotAcct.replace('.','_')

  # State of the slot
  addMetricCollect( collectorName, collectorSite, "slots.%s" % stateSlot , 1 )

  # State of the cpus
  addMetricCollect( collectorName, collectorSite, "cpus.%s"  % stateCpus , slotCpus )


  # Save the Owner if one is assigned
  if ( slotOwner != '' ) :
    addMetricCollect( collectorName, collectorSite, "owner.%s.Running"   % _slotOwner )
    addMetricCollect( collectorName, collectorSite, "owner.%s.Cores"     % _slotOwner , slotCpus )

  # Save the AccountingGroup statistics if one is assigned
  if ( slotAcct != '' ) :
    addMetricCollect( collectorName, collectorSite, "account.%s.Running" % _slotAcct )
    addMetricCollect( collectorName, collectorSite, "account.%s.Cores"   % _slotAcct , slotCpus )


################################################################################
# Pass 4 - Build the Job Collect Metrics from the data we have collected
################################################################################

# Job metrics
for jobGlobalJobID in metricJobDICT :

  # If this job was found in a Slot but not in a Schedd, ignore it and continue
  if ( not metricJobDICT[jobGlobalJobID]['Validated'] ) :
#   print >>sys.stderr, '%s Ignoring Job which was never validated: %s' % (time.strftime("%Y-%m-%d %H:%M:%S"), jobGlobalJobID)
    continue

  # Retrive all the job metrics
  jobSchedd            = metricJobDICT[jobGlobalJobID]['Schedd'] 
  jobHost              = metricJobDICT[jobGlobalJobID]['Host'] 
  jobQueue             = metricJobDICT[jobGlobalJobID]['Queue'] 
  jobCores             = metricJobDICT[jobGlobalJobID]['Cores'] 
  jobCpuTime           = metricJobDICT[jobGlobalJobID]['CpuTime'] 
  jobWallClockTime     = metricJobDICT[jobGlobalJobID]['WallClockTime'] 
  jobCoreWallClockTime = metricJobDICT[jobGlobalJobID]['CoreWallClockTime'] 
  jobEfficiency        = metricJobDICT[jobGlobalJobID]['Efficiency'] 
  jobCollectorName     = metricJobDICT[jobGlobalJobID]['CollectorName'] 
  jobCollectorSite     = metricJobDICT[jobGlobalJobID]['CollectorSite'] 

  jobPool              = metricJobDICT[jobGlobalJobID]['SlotPool']
  jobOwner             = metricJobDICT[jobGlobalJobID]['SlotOwner']
  jobAcct              = metricJobDICT[jobGlobalJobID]['SlotAccountingGroup'] 

  # A few failbacks if the Job ClassAD did not give us any information
  if ( jobPool  == '' ) : jobPool  = metricJobDICT[jobGlobalJobID]['JobPool']
  if ( jobOwner == '' ) : jobOwner = metricJobDICT[jobGlobalJobID]['JobOwner']
  if ( jobAcct  == '' ) : jobAcct  = metricJobDICT[jobGlobalJobID]['JobAccountingGroup']
# if ( jobPool  == '' ) : jobPool  = metricJobDICT[jobGlobalJobID]['SlotPool']
# if ( jobOwner == '' ) : jobOwner = metricJobDICT[jobGlobalJobID]['SlotOwner']
# if ( jobAcct  == '' ) : jobAcct  = metricJobDICT[jobGlobalJobID]['SlotAccountingGroup']

  # Replace '.' with "_' for those entries used by graphite
  _jobCollectorSite = jobCollectorSite.replace('.','_')
  _jobPool          = jobPool.replace('.','_')
  _jobAcct          = jobAcct.replace('.','_')
  _jobHost          = jobHost.replace('.','_')
  _jobOwner         = jobOwner.replace('.','_')


  # Sum the Pool metrics
  metricPoolKey = '%s.%s.metrics' % (_jobCollectorSite, _jobPool)
  addMetricDICT( metricPoolDICT , metricPoolKey , 'CpuTime'           , jobCpuTime           )
  addMetricDICT( metricPoolDICT , metricPoolKey , 'Cores'             , jobCores             )
  addMetricDICT( metricPoolDICT , metricPoolKey , 'WallClockTime'     , jobWallClockTime     )
  addMetricDICT( metricPoolDICT , metricPoolKey , 'CoreWallClockTime' , jobCoreWallClockTime )

  # Sum the AccountingGroup metrics (Must have a valid Account)
  if (jobAcct != '') :
    metricAcctKey = '%s.%s.account.%s'  % (_jobCollectorSite, _jobPool, _jobAcct )
    addMetricDICT( metricAcctDICT , metricAcctKey , 'CpuTime'           , jobCpuTime           )
    addMetricDICT( metricAcctDICT , metricAcctKey , 'Cores'             , jobCores             )
    addMetricDICT( metricAcctDICT , metricAcctKey , 'WallClockTime'     , jobWallClockTime     )
    addMetricDICT( metricAcctDICT , metricAcctKey , 'CoreWallClockTime' , jobCoreWallClockTime )

  # Sum the Host metrics (Must have a valid host)
  if (jobHost != '') :
    metricHostKey = '%s.%s.host.%s' % (_jobCollectorSite, _jobPool, _jobHost)
    addMetricDICT( metricHostDICT , metricHostKey , 'CpuTime'           , jobCpuTime           )
    addMetricDICT( metricHostDICT , metricHostKey , 'Cores'             , jobCores             )
    addMetricDICT( metricHostDICT , metricHostKey , 'WallClockTime'     , jobWallClockTime     )
    addMetricDICT( metricHostDICT , metricHostKey , 'CoreWallClockTime' , jobCoreWallClockTime )

  # Sum the Onwer metrics (Must have a valid Owner)
  if (jobOwner != '') :
    metricOwnerKey = '%s.%s.owner.%s' % (_jobCollectorSite, _jobPool, _jobOwner)
    addMetricDICT( metricOwnerDICT , metricOwnerKey , 'CpuTime'           , jobCpuTime           )
    addMetricDICT( metricOwnerDICT , metricOwnerKey , 'Cores'             , jobCores             )
    addMetricDICT( metricOwnerDICT , metricOwnerKey , 'WallClockTime'     , jobWallClockTime     )
    addMetricDICT( metricOwnerDICT , metricOwnerKey , 'CoreWallClockTime' , jobCoreWallClockTime )


################################################################################
# Pass 5 - Build the Schedd Collect Metrics from the data
################################################################################

## Currently we are not doing anything with Schedd metrics and they are not complete

# Schedd metrics
#for scheddName in metricScheddDICT :
#
#  # Retrive all the Schedd metrics
#  scheddRunning         = metricScheddDICT[scheddName]['Running'] 
#  scheddCores           = metricScheddDICT[scheddName]['Cores'] 
#
#  addMetricCollect( collectorName, collectorSite, "schedd.%s.Running" % scheddName, scheddRunning )
#  addMetricCollect( collectorName, collectorSite, "schedd.%s.Cores"   % scheddName, scheddCores   )


################################################################################


################################################################################
# Pass 6 - Compute the efficiencies for all categories
################################################################################

## Compute and store the efficiencies into the Collect Metric
effMetricDICT( metricPoolDICT  )
effMetricDICT( metricAcctDICT  )
effMetricDICT( metricHostDICT  )
effMetricDICT( metricOwnerDICT )


################################################################################
# Pass 7 - Output to stdout the Collect Metrics with the graphite base and timestamp
################################################################################

# Get the current time
timestamp = int(time.time())

# Loop over each metric and display its total and the current time
for metricName in metricCollectDICT :
  metricCount = metricCollectDICT[metricName]
  print "%s.%s %s %s" % (graphBase, metricName, metricCount, timestamp)





