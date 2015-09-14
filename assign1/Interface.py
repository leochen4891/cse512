#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2

DATABASE_NAME = 'dds_assgn1'
RATINGS_TABLE = 'ratings'

POWER = 2
INPUT_FILE_PATH = 'test_data{0}.dat'.format(POWER)
ACTUAL_ROWS_IN_INPUT_FILE = 10**POWER  # Number of lines in the input file

USER_ID_COLNAME = 'userid'
MOVIE_ID_COLNAME = 'movieid'
RATING_COLNAME = 'rating'


def getopenconnection(user='postgres', password='1234', dbname='dds_assgn1'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadratings(ratingstablename, ratingsfilepath, openconnection):
    f = None
    cur = None
    try:
        cur = openconnection.cursor()
        cur.execute("DROP TABLE IF EXISTS " + ratingstablename)
        createTableCmd = 'CREATE TABLE {0}({1} INTEGER, {2} INTEGER, {3} FLOAT)'\
            .format(ratingstablename, USER_ID_COLNAME, MOVIE_ID_COLNAME, RATING_COLNAME)
        cur.execute(createTableCmd)
        print 'reading records from ', ratingsfilepath
        f = open(ratingsfilepath, "r")

        line = f.readline()
        while line:

            strs = line.split('::')

            # insertCmd = 'INSERT INTO {0} VALUES({1},{2},{3},{4})'.format(RATINGS_TABLE, strs[0], strs[1], strs[2],strs[3])
            insertCmd = 'INSERT INTO {0} VALUES({1},{2},{3})'.format(RATINGS_TABLE, *strs)
            # print insertCmd
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
    pass


def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    pass


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    pass


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    pass


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
        #print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.close()


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

        # Use this function to do any set up after I finish testing, if you want to
        after_test_script_ends_middleware(con, DATABASE_NAME)

    except Exception as detail:
        print "OOPS! This is the error ==> ", detail
