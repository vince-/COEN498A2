#!/usr/bin/env python

"""	MapReduce.py: Simple MapReduce algorithm to process canadian government environmental data
    Vincent Bilodeau 26678955
	COEN498 , April 2016"""

""" REDUCE method """

__author__      = "Vincent Bilodeau"
__copyright__   = "Copyright 2016"

import sys
from itertools import groupby
from operator import itemgetter


def read_mapper_output(file, separator='\t'):
    for line in file:
        yield line.rstrip().split(separator, 1)

def main():
    tab = '\t'
    # input comes from STDIN (standard input)
    data = read_mapper_output(sys.stdin, tab)
    # groupby groups multiple cityYear-mBq pairs by cityYear and creates an iterator that returns consecutive keys and their yearlyGroup
    # the entire iterator results are fed to a list constructor because multiple operations are needed on each stripe of data
    for cityYear, yearlyGroup in groupby(data, itemgetter(0)):
        try:
            data = list((float(mBq) for cityYear, mBq in yearlyGroup))
            avg = str(sum(data)/len(data))
            maximum = str(max(data))
            minimum = str(min(data))
            print "%s%s%s%s%s%s%s" % (cityYear, tab, maximum, tab, minimum, tab, avg)
        except ValueError:
            # count was not a number, so silently discard this item
            pass

if __name__ == "__main__":
    main()