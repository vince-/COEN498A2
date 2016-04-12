#!/usr/bin/env python

"""     
    MapReduce driver: Simple MapReduce algorithm to process canadian government environmental data
    Vincent Bilodeau 26678955
    COEN498 , April 2016
"""

""" Driver method
    Requires an installed Hadoop execution environment
"""

__author__      = "Vincent Bilodeau"
__copyright__   = "Copyright 2016"

import sys
import os
import subprocess
from collections import defaultdict
import csv

""" Main process """
if __name__ == '__main__':
    print "Starting the MapReduce job"

    """
	Arguments are in this order : SOURCE_FILE DESTINATION_DIR MAPPER REDUCER
    """
    source_file = os.path.abspath(sys.argv[1])
    dest_file = os.path.abspath(sys.argv[2])
    mapper = os.path.abspath(sys.argv[3])
    reducer = os.path.abspath(sys.argv[4])

    cmd = "module load hadoop"
    subprocess.Popen(cmd, shell=True, executable="/bin/bash", stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
    cmd = "hadoop jar $HADOOP_DIR/contrib/streaming/hadoop-streaming-1.1.2.jar -input " + source_file + " -output " + dest_file + \
              " -file " + mapper + " -file " + reducer + " -mapper " + mapper + " -reducer " + reducer
    output,error = subprocess.Popen(cmd, shell=True, executable="/bin/bash", stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
    outFile = os.path.abspath(sys.argv[2]+'/part-00000')

    print "MapReduce job ended, accessing processed data"

    processed_data = open(outFile, 'rb')
    data = csv.reader(processed_data, delimiter='\t')
    output_data = defaultdict(list)

    for column in data:
	cityYear = column[0]
	maximum = column[1]
	minimum = column[2]
	avg = column[3]
	output_data[cityYear].append(maximum)
	output_data[cityYear].append(minimum)
	output_data[cityYear].append(avg)

    try:
	while(1):
		usr_input = raw_input('Please enter the year, an empty entry will quit the program: \n')
		if usr_input == '':
			sys.exit()
		year = usr_input
		city = raw_input('Please enter the city: \n')
		key = city+'-'+year		
		results = output_data[key]
		print "The maximum metric mBq/m3 of Cesium134 in " + city + " during " + year + " is " + str(results[0])
		print "The minimum metric mBq/m3 of Cesium134 in " + city + " during " + year + " is " + str(results[1])
		print "The average metric mBq/m3 of Cesium134 in " + city + " during " + year + " is " + str(results[2])
    except ValueError:
		print "Oops!  That was not a valid entry.  Try again..."
