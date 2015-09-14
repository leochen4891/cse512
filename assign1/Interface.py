#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2
import math

DATABASE_NAME = 'dds_assgn1'
RATINGS_TABLE = 'ratings'

POWER = 2
INPUT_FILE_PATH = 'test_data{0}.dat'.format(POWER)
ACTUAL_ROWS_IN_INPUT_FILE = 10**POWER  # Number of lines in the input file

USER_ID_COLNAME = 'userid'
MOVIE_ID_COLNAME = 'movieid'
RATING_COLNAME = 'rating'

RANGE_TABLE_PREFIX = 'range_part'
RROBIN_TABLE_PREFIX = 'rrobin_part'

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

roundrobinPartition_TableName2NumberOfPartitionsMap = {}
RANGE_PART_NAMES = []
nextrobin = 0

def getopenconnection(user='postgres', password='1234', dbname='dds_assgn1'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


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





def create_db(dbname):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getopenconnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        pass
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.close()

def create_table(tablename, openconnection):
    cur = openconnection.cursor()
    cur.execute("DROP TABLE IF EXISTS " + tablename)
    createTableCmd = 'CREATE TABLE {0}({1} INTEGER, {2} INTEGER, {3} FLOAT)'\
        .format(tablename, USER_ID_COLNAME, MOVIE_ID_COLNAME, RATING_COLNAME)
    # print createTableCmd
    cur.execute(createTableCmd)
    openconnection.commit()

def deletepartitionsandexit(openconnection):
    cur = openconnection.cursor()

    for i in range(1,12):
        cur.execute("DROP TABLE IF EXISTS " + '{0}{1}'.format(RANGE_TABLE_PREFIX, i))
        cur.execute("DROP TABLE IF EXISTS " + '{0}{1}'.format(RROBIN_TABLE_PREFIX, i))
    openconnection.commit()
    cur.close

# Middleware
def before_db_creation_middleware():
    # Use it if you want to
    pass


def after_db_creation_middleware(databasename):
    # Use it if you want to
    pass


def before_test_script_starts_middleware(openconnection, databasename):
    # Use it if you want to
    pass


def after_test_script_ends_middleware(openconnection, databasename):
    # Use it if you want to
    pass


if __name__ == '__main__':
    try:

        # Use this function to do any set up before creating the DB, if any
        before_db_creation_middleware()

        create_db(DATABASE_NAME)

        # Use this function to do any set up after creating the DB, if any
        after_db_creation_middleware(DATABASE_NAME)

        con = getopenconnection()
        # Use this function to do any set up before I starting calling your functions to test, if you want to
        before_test_script_starts_middleware(con, DATABASE_NAME)

        # Here is where I will start calling your functions to test them. For example,
        loadratings(RATINGS_TABLE, INPUT_FILE_PATH, con)
        # ###################################################################################
        # Anything in this area will not be executed as I will call your functions directly
        # so please add whatever code you want to add in main, in the middleware functions provided "only"
        # ###################################################################################
        deletepartitionsandexit(con)

        # Use this function to do any set up after I finish testing, if you want to
        after_test_script_ends_middleware(con, DATABASE_NAME)

    except Exception as detail:
        print "OOPS! This is the error ==> ", detail
