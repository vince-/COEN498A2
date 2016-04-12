#!/usr/bin/env python

"""	MapReduce.py: Simple MapReduce algorithm to process canadian government environmental data
    Vincent Bilodeau 26678955
	COEN498 , April 2016"""

""" MAP method """

__author__      = "Vincent Bilodeau"
__copyright__   = "Copyright 2016"

import sys

def read_input(file):
    for line in file:
        # split the line into words
        yield line.split(',')

def main():
    # input comes from STDIN (standard input)
    data = read_input(sys.stdin)
    for column in data:
    	try:	
	     	city = column[0]
	     	if city == 'Location/Emplacement':
	     		continue
	     	date = column[2].split('-')
	     	year = date[0]
	     	key = city + '-' + year
	     	cesium = column[13]
	     	if cesium != '':
	     		print '%s%s%s' % (key, '\t', cesium)
     	except ValueError:
     		#Skip the header
     		continue

if __name__ == "__main__":
    main()
