#!/usr/bin/python2.7
#
# Interface for the assignement
#

__author__ = 'Lei Chen'

'''
NOTE! these functions are not thread safe
'''

import psycopg2
import math

DATABASE_NAME = 'dds_assgn1'
RATINGS_TABLE = 'ratings'

POWER = 4
INPUT_FILE_PATH = 'test_data{0}.dat'.format(POWER)
ACTUAL_ROWS_IN_INPUT_FILE = 10**POWER  # Number of lines in the input file

USER_ID_COLNAME = 'userid'
MOVIE_ID_COLNAME = 'movieid'
RATING_COLNAME = 'rating'

RANGE_TABLE_PREFIX = 'range_part'
RROBIN_TABLE_PREFIX = 'rrobin_part'

RANGE_TABLE_INDEX_START = 1
RROBIN_TABLE_INDEX_START = 0

range_numberofpartitions = 0
range_part_names = []

rrobin_numberofpartitions = 0
rrobin_part_names = []
nextrobin = 0


# Since only have 11 possbile values, use a static table to avoid duplicated computation
# for numberofpartitions in range(1,12): rangeIndices = [min(numberofpartitions-1, int((float(d)/2)/(5.0/numberofpartitions))) for d in range(0,11)]
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

def getopenconnection(user='postgres', password='1234', dbname='dds_assgn1'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

#------------------------------------------------------------
#               load ratings into the database
#------------------------------------------------------------
def loadratings(ratingstablename, ratingsfilepath, openconnection):
    f = None
    cur = None
    try:
        openconnection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE)

        deletepartitions(openconnection)
        create_table(ratingstablename, openconnection)
        cur = openconnection.cursor()
        # print 'reading records from ', ratingsfilepath
        f = open(ratingsfilepath, "r")
        line = f.readline()
        while line:
            strs = line.split('::')
            insertCmd = 'INSERT INTO {0} VALUES({1},{2},{3})'.format(RATINGS_TABLE, *strs)
            # print insertCmd
            cur.execute(insertCmd)
            line = f.readline()

        openconnection.commit()
        openconnection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    except Exception as detail:
        print 'loadratings from file:', ratingsfilepath, 'failed:', detail
        openconnection.rollback()
    finally:
        if f: f.close()
        if cur: cur.close()

#------------------------------------------------------------
#            partition the table based on rating
#------------------------------------------------------------
def rangepartition(ratingstablename, numberofpartitions, openconnection):
    cur = None
    try :
        # total 11 rating values, from 0.0 to 5.0 by 0.5 increase. larger ones don't make sense
        if not isinstance(numberofpartitions, int) or numberofpartitions <= 0 or numberofpartitions > 11:
            raise ValueError("number of partitions must be positive integer less than or equal to 11")

        # LAME DESIGN: not thread safe
        global range_numberofpartitions
        range_numberofpartitions = numberofpartitions

        global range_part_names
        range_part_names = [RANGE_TABLE_PREFIX + str(i) for i in range(RANGE_TABLE_INDEX_START,RANGE_TABLE_INDEX_START + numberofpartitions)]

        openconnection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE)

        cur = openconnection.cursor()
        # create tables
        for part_name in range_part_names:
            dropTableCmd = 'DROP TABLE IF EXISTS '+ part_name
            cur.execute(dropTableCmd)

            createTableCmd = 'CREATE TABLE {0}({1} INTEGER, {2} INTEGER, {3} FLOAT)'.format(part_name, USER_ID_COLNAME, MOVIE_ID_COLNAME, RATING_COLNAME)
            cur.execute(createTableCmd)

        # insert data records based on rating
        rangeIndices = RANGE_INDICES[numberofpartitions-1]
        cur.execute('SELECT * from {0}'.format(ratingstablename))
        rows = cur.fetchall()
        for row in rows:
            rating = row[2]
            index = int(rating*2)
            part_name = range_part_names[rangeIndices[index]]
            insertCmd = 'INSERT INTO {0} VALUES({1},{2},{3})'.format(part_name, *row)
            # print(insertCmd)
            cur.execute(insertCmd)

        openconnection.commit()
        openconnection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

    except Exception as detail:
        print 'range partition failed:', detail
        openconnection.rollback()
    finally:
        if cur: cur.close()

#------------------------------------------------------------
#            insert an item based on rating
#------------------------------------------------------------
def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    cur = None
    try :
        global range_numberofpartitions
        global range_part_names

        if rating > 5.0 or rating < 0.0:
            print 'rating ', rating, ' is not between 0.0 and 5.0'
            return

        rangeIndices = RANGE_INDICES[range_numberofpartitions-1]
        index = int(rating*2)
        part_name = range_part_names[rangeIndices[index]]

        openconnection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE)

        cur = openconnection.cursor()
        insertCmd = 'INSERT INTO {0} VALUES({1},{2},{3})'.format(part_name, userid, itemid, rating)
        # print(insertCmd)
        cur.execute(insertCmd)

        openconnection.commit()
        openconnection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

    except Exception as detail:
        print 'range insert failed:', detail
        openconnection.rollback()
    finally:
        if cur: cur.close()

#------------------------------------------------------------
#            partition the table using roundrobin
#------------------------------------------------------------
def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    cur = None
    try :
        # total 11 rating values, from 0.0 to 5.0 by 0.5 increase. larger ones don't make sense
        if not isinstance(numberofpartitions, int) or numberofpartitions <= 0 or numberofpartitions > 11:
            raise ValueError("number of partitions must be positive integer less than or equal to 11")

        # LAME DESIGN: not thread safe
        global rrobin_numberofpartitions
        rrobin_numberofpartitions = numberofpartitions

        global rrobin_part_names
        rrobin_part_names = [RROBIN_TABLE_PREFIX + str(i) for i in range(RROBIN_TABLE_INDEX_START,RROBIN_TABLE_INDEX_START + numberofpartitions)]

        global nextrobin

        openconnection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE)

        # create tables
        cur = openconnection.cursor()
        for part_name in rrobin_part_names:
            dropTableCmd = 'DROP TABLE IF EXISTS '+ part_name
            cur.execute(dropTableCmd)

            createTableCmd = 'CREATE TABLE {0}({1} INTEGER, {2} INTEGER, {3} FLOAT)'.format(part_name, USER_ID_COLNAME, MOVIE_ID_COLNAME, RATING_COLNAME)
            cur.execute(createTableCmd)

        # insert data records based on rating
        cur.execute('SELECT * from {0}'.format(ratingstablename))
        rows = cur.fetchall()
        for row in rows:
            part_name = rrobin_part_names[nextrobin]
            insertCmd = 'INSERT INTO {0} VALUES({1},{2},{3})'.format(part_name, *row)
            # print(insertCmd)
            cur.execute(insertCmd)
            nextrobin = 0 if nextrobin >= rrobin_numberofpartitions - 1 else nextrobin + 1

        openconnection.commit()
        openconnection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

    except Exception as detail:
        print 'roundrobin partition failed:', detail
        openconnection.rollback()
    finally:
        if cur: cur.close()

#------------------------------------------------------------
#            insert an item using roundrobin
#------------------------------------------------------------
def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    cur = None
    try :
        if rating > 5.0 or rating < 0.0:
            print 'rating ', rating, ' is not between 0.0 and 5.0'
            return

        global rrobin_numberofpartitions
        global rrobin_part_names
        global nextrobin

        part_name = rrobin_part_names[nextrobin]

        openconnection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE)

        cur = openconnection.cursor()
        insertCmd = 'INSERT INTO {0} VALUES({1},{2},{3})'.format(part_name, userid, itemid, rating)
        # print(insertCmd)
        cur.execute(insertCmd)
        nextrobin = 0 if nextrobin >= rrobin_numberofpartitions - 1 else nextrobin + 1

        openconnection.commit()
        openconnection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

    except Exception as detail:
        print 'roundrobin insert failed:', detail
        openconnection.rollback()
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
    # con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
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
    cur = None
    try:
        cur = openconnection.cursor()
        cur.execute("DROP TABLE IF EXISTS " + tablename)
        createTableCmd = 'CREATE TABLE {0}({1} INTEGER, {2} INTEGER, {3} FLOAT)'\
            .format(tablename, USER_ID_COLNAME, MOVIE_ID_COLNAME, RATING_COLNAME)
        # print createTableCmd
        cur.execute(createTableCmd)
        openconnection.commit()
    finally:
        if cur: cur.close()

def deletepartitions(openconnection):
    cur = None
    try:
        cur = openconnection.cursor()
        for i in range(0,12):
            cur.execute("DROP TABLE IF EXISTS " + '{0}{1}'.format(RANGE_TABLE_PREFIX, i))
            cur.execute("DROP TABLE IF EXISTS " + '{0}{1}'.format(RROBIN_TABLE_PREFIX, i))
        openconnection.commit()
    finally:
        if cur: cur.close()

def deletepartitionsandexit(openconnection):
    deletepartitions(openconnection)

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

        rangepartition(RATINGS_TABLE, 3, con)
        for r in range(0, 11):
            rating = float(r)/2
            rangeinsert(RATINGS_TABLE, 100, 1000, rating, con)


        roundrobinpartition(RATINGS_TABLE, 3, con)
        roundrobininsert(RATINGS_TABLE, 100, 1000, 1.0, con)
        roundrobininsert(RATINGS_TABLE, 100, 1000, 2.0, con)
        roundrobininsert(RATINGS_TABLE, 100, 1000, 3.0, con)
        roundrobininsert(RATINGS_TABLE, 100, 1000, 4.0, con)
        roundrobininsert(RATINGS_TABLE, 100, 1000, 5.0, con)

        # ###################################################################################
        # Anything in this area will not be executed as I will call your functions directly
        # so please add whatever code you want to add in main, in the middleware functions provided "only"
        # ###################################################################################

        # Use this function to do any set up after I finish testing, if you want to
        after_test_script_ends_middleware(con, DATABASE_NAME)

    except Exception as detail:
        print "OOPS! This is the error ==> ", detail
