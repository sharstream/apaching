###############################
####                      ####
### Data Log Analyzer    ###
##                      ##
#                      #
#######################

import re
import sys
import os
import datetime
from pyspark import SparkConf, SparkContext
import numpy
from py4j.java_gateway import JavaGateway
from py4j.tests.java_dir_test import gateway
gateway = JavaGateway()

groupId = 'org.apache.spark'
artifactId = 'spark-core_2.10'
version = '1.5.1'
# conf = (SparkConf()
#          .setMaster("local")
#          .setAppName("My app")
#          .set("spark.executor.memory", "1g"))
sc = SparkContext(conf = SparkConf)

global APACHE_ACCESS_LOG_PATTERN
APACHE_ACCESS_LOG_PATTERN = '^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)\s*(\S*)" (\d{3}) (\S+)'

from pyspark.sql import Row
month_map = {'Jan':1, 'Feb':2, 'Mar':3, 'Apr':4, 'May':5, 'Jun':6, 'Jul':7, 'Aug':8, 'Sep':9, 'Oct':10, 'Nov':11, 'Dec':12}

def parse_apache_time(s):
    return datetime.datetime(int(s[7:11]),
                             month_map[s[3:6]],
                             int(s[0:2]),
                             int(s[12:14]),
                             int(s[15:17]),
                             int(s[18:20]))
    
def parseApacheLogLine(logline):
    match = re.search(APACHE_ACCESS_LOG_PATTERN, logline)
    if match is None:
        return (logline, 0)
    size_field = match.group(9)
    if size_field == '-':
        size = long(0)
    else:
        size = long(match.group(9))
    return (Row(
        host            = match.group(1),
        client_identd   = match.group(2),
        user_id         = match.group(3),
        date_time       = parse_apache_time(match.group(4)),
        method          = match.group(5),
        endpoint        = match.group(6),
        protocol        = match.group(7),
        response_code   = int(match.group(8)),
        content_size    = size
    ),1)
    
baseDir = os.path.join('data')
inputPath = os.path.join('cs100', 'lab2', 'apache.access.log.PROJECT')
logFile = os.path.join(baseDir, inputPath)

def parseLogs():
    """ Read and parse log file """
    parsed_logs = (sc
                   .textFile(logFile)
                   .map(parseApacheLogLine)
                   .cache())

    access_logs = (parsed_logs
                   .filter(lambda s: s[1] == 1)
                   .map(lambda s: s[0])
                   .cache())

    failed_logs = (parsed_logs
                   .filter(lambda s: s[1] == 0)
                   .map(lambda s: s[0]))
    failed_logs_count = failed_logs.count()
    if failed_logs_count > 0:
        print 'Number of invalid logline: %d' % failed_logs.count()
        for line in failed_logs.take(20):
            print 'Invalid logline: %s' % line

    print 'Read %d lines, successfully parsed %d lines, failed to parse %d lines' % (parsed_logs.count(), access_logs.count(), failed_logs.count())
    return parsed_logs, access_logs, failed_logs


parsed_logs, access_logs, failed_logs = parseLogs()