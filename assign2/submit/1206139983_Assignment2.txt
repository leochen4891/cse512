#!/usr/bin/python2.7
#
# Assignment2 Lei Chen (1206139983)
#

import psycopg2
import os
import sys
import datetime
import time

import Assignment1 as Assignment1

'''
To Grader:

please set FORCE_READ_METADATA to True if metadata needs to be read before each query.
This may harm the performance.

'''

# --------------------------------------------------------------------------------
# -------------------------------- Constants -------------------------------------
# --------------------------------------------------------------------------------
FORCE_READ_METADATA = False

RANGE_PREFIX = 'rangeratingspart'
RANGE_METADATA = "rangeratingsmetadata"

RROBIN_PREFIX = 'roundrobinratingspart'
RROBIN_METADATA = 'roundrobinratingsmetadata'


# --------------------------------------------------------------------------------
# --------------------- a class for holding ranges -------------------------------
# --------------------------------------------------------------------------------
class MyRange:
    def __init__(self, row):
        self.index = row[0]
        self.left = row[1]
        self.right = row[2]

        # LAME DESIGN
        # the encoding of the range is not consistent, the last 5.0 is inclusive, while all others are exclusive
        # set it to 5.1 to bypass this problem
        if self.right == 5.0:
            self.right = 5.1

    def contains(self, value):
        return self.left <= value < self.right

    def intersects(self, left, right):
        if left < self.right and right > self.left:
            return max(left, self.left), min(right, self.right)
        return None

    def getIndex(self):
        return self.index

    def __str__(self):
        return "[" + str(self.left) + ", " + str(self.right) + "] --> " + str(self.index)


# --------------------------------------------------------------------------------
# --------------------------------- saved metadata -------------------------------
# --------------------------------------------------------------------------------
ranges = []
roundrobins = -1


# --------------------------------------------------------------------------------
# -------------------- functions to read the metadata ----------------------------
# --------------------------------------------------------------------------------
def ReadRangesMetadata(openconnection):
    global ranges
    cur = openconnection.cursor()
    cur.execute('SELECT * from {0}'.format(RANGE_METADATA))
    rows = cur.fetchall()

    for row in rows:
        ranges.append(MyRange(row))

    # print "Range Partitions are:"
    # for r in ranges:
    #     print(r)

def ReadRoundRobinMetadata(openconnection):
    global roundrobins

    cur = openconnection.cursor()
    cur.execute('SELECT * from {0}'.format(RROBIN_METADATA))
    row = cur.fetchone()
    roundrobins = row[0]
    # print "Round Robin Partitions Count = " + str(roundrobins)


# --------------------------------------------------------------------------------
# --------------------------- Range Query function -------------------------------
# --------------------------------------------------------------------------------
def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
    global ranges
    global roundrobins
    cur = None

    # check input
    if not isinstance(ratingMinValue, float) or not isinstance(ratingMaxValue, float):
        raise ValueError("rating value must be of type float")

    if ratingMinValue < 0:
        raise ValueError("ratingMinValue < 0")

    if ratingMaxValue > 5:
        raise ValueError("ratingMaxValue > 5")

    if ratingMinValue > ratingMaxValue:
        raise ValueError("ratingMinValue > ratingMaxValue")

    try:
        cur = openconnection.cursor()

        # open the output file
        output = open('RangeQueryOut.txt', 'w')

        # --------------------- query range partitions --------------------------
        # read the metadata
        if FORCE_READ_METADATA or not ranges or len(ranges) <= 0:
            ReadRangesMetadata(openconnection)

        # for each range, get the intersected range and output to file
        for r in ranges:
            if (r.intersects(ratingMinValue, ratingMaxValue)):
                left, right = r.intersects(ratingMinValue, ratingMaxValue)
                partName = RANGE_PREFIX + str(r.getIndex())
                # print "getting items ranging from", left, "to", right, "in", partName

                cur.execute('SELECT * from {0}'.format(partName))
                rows = cur.fetchall()
                for row in rows:
                    # print row,
                    if  left <= row[2] <= right:
                        output.write(partName + ", " + str(row[0]) + ", " + str(row[1]) + ", "+ str(row[2]) + "\n")
                        # print ''
                    else:
                        # print " ignored"
                        pass


        # ----------------------- query roundrobin partitions --------------------------
        if FORCE_READ_METADATA or roundrobins < 0:
            ReadRoundRobinMetadata(openconnection)

        for index in range(roundrobins):
            partName = RROBIN_PREFIX + str(index)
            # print "getting items ranging from", ratingMinValue, "to", ratingMaxValue, "in", partName

            cur.execute('SELECT * from {0}'.format(partName))
            rows = cur.fetchall()
            for row in rows:
                # print row,
                if  ratingMinValue <= row[2] <= ratingMaxValue:
                    output.write(partName + ", " + str(row[0]) + ", " + str(row[1]) + ", "+ str(row[2]) + "\n")
                    # print ''
                else:
                    # print " ignored"
                    pass

    except Exception as detail:
        print 'RangeQuery', ratingsTableName, ratingMinValue, 'to', ratingMaxValue, 'failed:', detail
    finally:
        output.close()


# --------------------------------------------------------------------------------
# --------------------------- Point Query function -------------------------------
# --------------------------------------------------------------------------------
def PointQuery(ratingsTableName, ratingValue, openconnection):
    global ranges
    global roundrobins

    # check input
    cur = None
    if not isinstance(ratingValue, float):
        raise ValueError("rating value must be of type float")

    if not 0 <= ratingValue <= 5:
        raise ValueError("ratingValue should be in [0, 5]")


    try:
        cur = openconnection.cursor()

        # open the output file
        output = open('PointQueryOut.txt', 'w')

        # --------------------- query range partitions --------------------------
        # read the metadata
        if FORCE_READ_METADATA or not ranges or len(ranges) <= 0:
            ReadRangesMetadata(openconnection);

        # for each range, get the intersected range and output to file
        for r in ranges:
            if (r.contains(ratingValue)):
                partName = RANGE_PREFIX + str(r.getIndex())
                # print "getting items of ranging", ratingValue, "in", partName

                cur.execute('SELECT * from {0}'.format(partName))
                rows = cur.fetchall()
                for row in rows:
                    # print row,
                    if  row[2] == ratingValue:
                        output.write(partName + ", " + str(row[0]) + ", " + str(row[1]) + ", "+ str(row[2]) + "\n")
                        # print ''
                    else:
                        # print " ignored"
                        pass


        # ----------------------- query roundrobin partitions --------------------------
        if FORCE_READ_METADATA or roundrobins < 0:
            ReadRoundRobinMetadata(openconnection)

        for index in range(roundrobins):
            partName = RROBIN_PREFIX + str(index)
            # print "getting items of ranging", ratingValue, "in", partName

            cur.execute('SELECT * from {0}'.format(partName))
            rows = cur.fetchall()
            for row in rows:
                # print row,
                if  row[2] == ratingValue:
                    output.write(partName + ", " + str(row[0]) + ", " + str(row[1]) + ", "+ str(row[2]) + "\n")
                    # print ''
                else:
                    # print " ignored"
                    pass

    except Exception as detail:
        print 'PointQuery', ratingsTableName, ratingValue, 'failed:', detail


# --------------------------------------------------------------------------------
# ------------------------- Test the performance -------------------------------
# --------------------------------------------------------------------------------

# utilities (code from assign1)
def getformattedtime(srctime):
    return datetime.datetime.fromtimestamp(srctime).strftime('%Y-%m-%d %H:%M:%S')

def formattedprint(message, newlineafter=False):
    if newlineafter:
        print("T: {0} {1}\n".format(getformattedtime(time.time()), message))
    else:
        print("T: {0} {1}".format(getformattedtime(time.time()), message))


# Decorators (code from assign1)
def timeme(func):
    def timeme_and_call(*args, **kwargs):
        tic = time.time()
        res = func(*args, **kwargs)
        toc = time.time()
        formattedprint('Took %2.5fs for "%r()"' % (toc - tic, func.__name__))
        return res

    return timeme_and_call

@timeme
def test(con):
    for i in range(100):
        PointQuery('ratings', 3.0, con);

# con = Assignment1.getOpenConnection();
# test(con)
