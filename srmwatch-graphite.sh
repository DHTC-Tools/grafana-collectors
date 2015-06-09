#!/bin/bash
# This query is set to run every 5 minutes. If adjusting the cron, please adjust the query as well

psql --username postgres --dbname dcache -c "select creationtime,state  from getfilerequests  where creationtime between '$(($(date +%s%3N) - 300000))' and '$(date +%s%3N)'" | awk '{print $3}' | grep "^[0-9]" | sort | uniq -c | awk -v date=`date +%s` '{print "stats.dcache.srm.getfilerequests.state."$2,$1,date}' | nc -w30 webapps.mwt2.org 2003
psql --username postgres --dbname dcache -c "select creationtime,state  from putfilerequests  where creationtime between '$(($(date +%s%3N) - 300000))' and '$(date +%s%3N)'" | awk '{print $3}' | grep "^[0-9]" | sort | uniq -c | awk -v date=`date +%s` '{print "stats.dcache.srm.putfilerequests.state."$2,$1,date}' | nc -w30 webapps.mwt2.org 2003 
