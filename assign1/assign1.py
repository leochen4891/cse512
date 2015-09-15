__author__ = 'user'

import psycopg2
USERNAME = 'postgres'
PASSWORD = '1234'
DATABASE_NAME = 'test_dds_assgn1'

RATINGS_TABLE = 'ratings'
RANGE_TABLE_PREFIX = 'range_part'
RROBIN_TABLE_PREFIX = 'rrobin_part'

USER_ID_COLNAME = 'userid'
MOVIE_ID_COLNAME = 'movieid'
RATING_COLNAME = 'rating'

POWER = 2
INPUT_FILE_PATH = 'test_data{0}.dat'.format(POWER)
ACTUAL_ROWS_IN_INPUT_FILE = 10**POWER  # Number of lines in the input file

def getopenconnection(user=USERNAME, password=PASSWORD, dbname=DATABASE_NAME):
    return psycopg2.connect("dbname='" + DATABASE_NAME + "' user='" + USERNAME + "' host='localhost' password='" + PASSWORD + "'")

def create_table(tablename, openconnection):
    cur = openconnection.cursor()
    cur.execute("DROP TABLE IF EXISTS " + tablename)

    # cur.execute("CREATE TABLE Cars(Id INTEGER PRIMARY KEY, Name VARCHAR(20), Price INT)")
    # cur.execute("INSERT INTO Cars VALUES(1,'Audi',52642)")
    createTableCmd = 'CREATE TABLE {0}({1} INTEGER, {2} INTEGER, {3} FLOAT)'\
        .format(tablename, USER_ID_COLNAME, MOVIE_ID_COLNAME, RATING_COLNAME)
    # print createTableCmd
    cur.execute(createTableCmd)
    openconnection.commit()

def loadratings(ratingstablename, ratingsfilepath, openconnection):
    f = None
    cur = None
    try:
        create_table(ratingstablename, openconnection)

        cur = openconnection.cursor()

        print 'reading records from ', ratingsfilepath
        f = open(ratingsfilepath, "r")

        line = f.readline()
        while line:

            strs = line.split('::')

            # insertCmd = 'INSERT INTO {0} VALUES({1},{2},{3},{4})'.format(RATINGS_TABLE, strs[0], strs[1], strs[2],strs[3])
            insertCmd = 'INSERT INTO {0} VALUES({1},{2},{3})'.format(RATINGS_TABLE, *strs)
            print insertCmd
            cur.execute(insertCmd)
            line = f.readline()

        print 'reading finished'

        print 'committing'
        openconnection.commit()
        print 'committing finished'

    except Exception as detail:
        print 'Read input file:', ratingsfilepath, 'failed:', detail
    finally:
        if f: f.close()
        if cur: cur.close()

import math
rangePartition_TableName2NumberOfPartitionsMap = {}
RANGE_INDICES = [
    [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    [0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1],
    [0, 0, 0, 0, 1, 1, 1, 2, 2, 2, 2],
    [0, 0, 0, 1, 1, 2, 2, 2, 3, 3, 3],
    [0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 4],
    [0, 0, 1, 1, 2, 3, 3, 4, 4, 5, 5],
    [0, 0, 1, 2, 2, 3, 4, 4, 5, 6, 6],
    [0, 0, 1, 2, 3, 4, 4, 5, 6, 7, 7],
    [0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 8],
    [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 9],
    [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]]
 #for numberofpartitions in range(1,12): rangeIndices = [min(numberofpartitions-1, int((float(d)/2)/(5.0/numberofpartitions))) for d in range(0,11)]

RANGE_PART_NAMES = []


def rangepartition(ratingstablename, numberofpartitions, openconnection):
    cur = None
    if not isinstance(numberofpartitions, int) or numberofpartitions <= 0:
        raise ValueError("number of partitions must be positive integer")

    numberofpartitions = min(numberofpartitions, 11) # total 11 rating values, from 0.0 to 5.0 by 0.5 increase.

    rangePartition_TableName2NumberOfPartitionsMap[ratingstablename] = numberofpartitions
    rangeIndices = RANGE_INDICES[numberofpartitions-1]

    global RANGE_PART_NAMES
    RANGE_PART_NAMES = [RANGE_TABLE_PREFIX + str(i) for i in range(1,numberofpartitions+1)]

    try:
        cur = openconnection.cursor()
        cur.execute('SELECT * from {0}'.format(ratingstablename))
        rows = cur.fetchall()

        for part_name in RANGE_PART_NAMES:
            createTableCmd = 'CREATE TABLE {0}({1} INTEGER, {2} INTEGER, {3} FLOAT)'\
                    .format(part_name, USER_ID_COLNAME, MOVIE_ID_COLNAME, RATING_COLNAME)
            print createTableCmd
            cur.execute(createTableCmd)
            openconnection.commit()

        for row in rows:
            rating = row[2]
            index = int(rating*2)
            part_name = RANGE_PART_NAMES[rangeIndices[index]]
            insertCmd = 'INSERT INTO {0} VALUES({1},{2},{3})'.format(part_name, *row)
            print(insertCmd)
            cur.execute(insertCmd)

        print 'committing'
        openconnection.commit()
        print 'committing finished'

    except Exception as detail:
        print 'range partition failed:', detail
    finally:
        if cur: cur.close()

def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    if not rangePartition_TableName2NumberOfPartitionsMap[ratingstablename]:
        print 'cannot find number of partitions for table:', ratingstablename
        return
    if rating > 5.0 or rating < 0.0:
        print 'rating ', rating, ' is not between 0.0 and 5.0'
        return

    numberofpartitions = rangePartition_TableName2NumberOfPartitionsMap[ratingstablename]

    rangeIndices = RANGE_INDICES[numberofpartitions-1]
    index = int(rating*2)
    part_name = RANGE_PART_NAMES[rangeIndices[index]]
    try:
        cur = openconnection.cursor()
        insertCmd = 'INSERT INTO {0} VALUES({1},{2},{3})'.format(part_name, userid, itemid, rating)
        print(insertCmd)
        cur.execute(insertCmd)
        openconnection.commit()
    finally:
        if cur: cur.close()


roundrobinPartition_TableName2NumberOfPartitionsMap = {}
RANGE_PART_NAMES = []
nextrobin = 0


def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    cur = None
    if not isinstance(numberofpartitions, int) or numberofpartitions <= 0:
        raise ValueError("number of partitions must be positive integer")

    numberofpartitions = min(numberofpartitions, 11) # total 11 rating values, from 0.0 to 5.0 by 0.5 increase.

    roundrobinPartition_TableName2NumberOfPartitionsMap[ratingstablename] = numberofpartitions

    global ROBIN_PART_NAMES
    global nextrobin
    try:
        cur = openconnection.cursor()
        cur.execute('SELECT * from {0}'.format(ratingstablename))
        rows = cur.fetchall()
        print 'size =', len(rows)
        nextrobin = 0
        ROBIN_PART_NAMES = [RROBIN_TABLE_PREFIX + str(i) for i in range(1,numberofpartitions+1)]

        for part_name in ROBIN_PART_NAMES:
            createTableCmd = 'CREATE TABLE {0}({1} INTEGER, {2} INTEGER, {3} FLOAT)'\
                    .format(part_name, USER_ID_COLNAME, MOVIE_ID_COLNAME, RATING_COLNAME)
            cur.execute(createTableCmd)
            openconnection.commit()

        for row in rows:
            if nextrobin >= numberofpartitions: nextrobin = 0
            part_name = ROBIN_PART_NAMES[nextrobin]
            insertCmd = 'INSERT INTO {0} VALUES({1},{2},{3})'.format(part_name, *row)
            print(insertCmd)
            cur.execute(insertCmd)
            nextrobin = (nextrobin + 1)

        print 'committing'
        openconnection.commit()
        print 'committing finished'

    except Exception as detail:
        print 'range partition failed:', detail
    finally:
        if cur: cur.close()

def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    if not roundrobinPartition_TableName2NumberOfPartitionsMap[ratingstablename]:
        print 'cannot find number of partitions for table:', ratingstablename
        return
    if rating > 5.0 or rating < 0.0:
        print 'rating ', rating, ' is not between 0.0 and 5.0'
        return

    numberofpartitions = roundrobinPartition_TableName2NumberOfPartitionsMap[ratingstablename]

    global nextrobin
    if nextrobin >= numberofpartitions: nextrobin = 0

    part_name = ROBIN_PART_NAMES[nextrobin]
    try:
        cur = openconnection.cursor()
        insertCmd = 'INSERT INTO {0} VALUES({1},{2},{3})'.format(part_name, userid, itemid, rating)
        print(insertCmd)
        cur.execute(insertCmd)
        openconnection.commit()
        nextrobin += 1
    finally:
        if cur: cur.close()


def deletepartitionsandexit(openconnection):
    cur = openconnection.cursor()

    for i in range(1,12):
        cur.execute("DROP TABLE IF EXISTS " + '{0}{1}'.format(RANGE_TABLE_PREFIX, i))
        cur.execute("DROP TABLE IF EXISTS " + '{0}{1}'.format(RROBIN_TABLE_PREFIX, i))
    openconnection.commit()
    cur.close


if __name__ == '__main__':
    con = None
    try:
        con = getopenconnection()

        deletepartitionsandexit(con)

        loadratings(RATINGS_TABLE, INPUT_FILE_PATH, con)
        rangepartition(RATINGS_TABLE, 11, con)
        # for r in range(0, 11):
        #     rating = float(r)/2
        #     rangeinsert(RATINGS_TABLE, 100, 1000, rating, con)

        roundrobinpartition(RATINGS_TABLE, 3, con)
        roundrobininsert(RATINGS_TABLE, 100, 1000, 1.0, con)
        roundrobininsert(RATINGS_TABLE, 100, 1000, 2.0, con)
        roundrobininsert(RATINGS_TABLE, 100, 1000, 3.0, con)
        roundrobininsert(RATINGS_TABLE, 100, 1000, 4.0, con)
        roundrobininsert(RATINGS_TABLE, 100, 1000, 5.0, con)


    except Exception as detail:
        print "OOPS! This is the error ==> ", detail

    finally:
        if con: con.close()
