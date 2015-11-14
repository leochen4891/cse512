#!/usr/bin/python2.7
#
# Assignment3 Interface
#

import psycopg2
import os
import sys

##################### This needs to changed based on what kind of table we want to sort. ##################
##################### To know how to change this, see Assignment 3 Instructions carefully #################
USER_ID_COLNAME = 'userid'
MOVIE_ID_COLNAME = 'movieid'
RATING_COLNAME = 'rating'
YEAR_ID_COLNAME = 'year'
COLLECTION_COLNAME = 'collection'

RATINGS_TABLE = 'movieratings'
RATINGS_TABLE_COLUMNS = [USER_ID_COLNAME, MOVIE_ID_COLNAME, RATING_COLNAME]

COLLECTIONS_TABLE = 'moviecollections'
COLLECTIONS_TABLE_COLUMNS = [MOVIE_ID_COLNAME, YEAR_ID_COLNAME, COLLECTION_COLNAME]

SORT_OUTPUT_TABLE_NAME = 'parallelSortOutputTable'
JOIN_OUTPUT_TABLE_NAME = 'parallelJoinOutputTable'

FIRST_TABLE_NAME = RATINGS_TABLE
SECOND_TABLE_NAME = COLLECTIONS_TABLE
SORT_COLUMN_NAME_FIRST_TABLE = RATING_COLNAME
SORT_COLUMN_NAME_SECOND_TABLE = COLLECTION_COLNAME
JOIN_COLUMN_NAME_FIRST_TABLE = MOVIE_ID_COLNAME
JOIN_COLUMN_NAME_SECOND_TABLE = MOVIE_ID_COLNAME

RATINGS_INPUT_FILE_PATH = 'movieratings.dat'
COLLECTION_INPUT_FILE_PATH = 'moviecollections.dat'
##########################################################################################################


# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
    #Implement ParallelSort Here.
    pass #Remove this once you are done with implementation

def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    #Implement ParallelJoin Here.
    pass # Remove this once you are done with implementation


################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='1234', dbname='ddsassignment3'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# Donot change this function
def createDB(dbname='ddsassignment3'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.commit()
    con.close()

''' LAME DESIGN: hard-coded'''
def create_tables(openconnection):
    cur = None

    try:
        cur = openconnection.cursor()
        cur.execute("DROP TABLE IF EXISTS " + RATINGS_TABLE)
        cur.execute("DROP TABLE IF EXISTS " + COLLECTIONS_TABLE)

        createTableCmd = 'CREATE TABLE ' + RATINGS_TABLE + '(' \
                         + USER_ID_COLNAME + ' INTEGER,' \
                         + MOVIE_ID_COLNAME + ' INTEGER,' \
                         + RATING_COLNAME + ' FLOAT)'
        print createTableCmd
        cur.execute(createTableCmd)

        createTableCmd = 'CREATE TABLE ' + COLLECTIONS_TABLE + '(' \
                     + MOVIE_ID_COLNAME + ' INTEGER,' \
                     + YEAR_ID_COLNAME + ' INTEGER,' \
                     + COLLECTION_COLNAME + ' INTEGER)'
        print createTableCmd
        cur.execute(createTableCmd)

        openconnection.commit()
    except Exception as detail:
        print 'create_tables failed:', detail
    finally:
        if cur: cur.close()


def load_files_to_table(openconnection):
    f = None
    cur = None
    try:
        create_tables(openconnection)
        cur = openconnection.cursor()

        # movie ratings table
        print 'reading records from ', RATINGS_INPUT_FILE_PATH
        f = open(RATINGS_INPUT_FILE_PATH, "r")

        line = f.readline()
        while line:
            strs = line.split('::')
            insertCmd = 'INSERT INTO {0} VALUES({1},{2},{3})'.format(RATINGS_TABLE, *strs)
            print insertCmd
            cur.execute(insertCmd)
            line = f.readline()

        # movie collections table
        print 'reading records from ', COLLECTION_INPUT_FILE_PATH
        f = open(COLLECTION_INPUT_FILE_PATH, "r")

        line = f.readline()
        while line:
            strs = line.split('::')
            insertCmd = 'INSERT INTO {0} VALUES({1},{2},{3})'.format(COLLECTIONS_TABLE, *strs)
            print insertCmd
            cur.execute(insertCmd)
            line = f.readline()

        openconnection.commit()

    except Exception as detail:
        print 'load_files_to_table failed:', detail
    finally:
        if f: f.close()
        if cur: cur.close()

# Donot change this function
def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

# Donot change this function
def saveTable(ratingstablename, fileName, openconnection):
    pass
    # try:
    #     cursor = openconnection.cursor()
    #     cursor.execute("Select * from %s" %(ratingstablename))
    #     data = cursor.fetchall()
    #     openFile = open(fileName, "w")
    #     for row in data:
    #         for d in row:
    #             openFile.write(`d`+",")
    #         openFile.write('\n')
    #     openFile.close()
    # except psycopg2.DatabaseError, e:
    #     if openconnection:
    #         openconnection.rollback()
    #     print 'Error %s' % e
    #     sys.exit(1)
    # except IOError, e:
    #     if openconnection:
    #         conn.rollback()
    #     print 'Error %s' % e
    #     sys.exit(1)
    # finally:
    #     if cursor:
    #         cursor.close()

if __name__ == '__main__':
    try:
        # Creating Database ddsassignment2
        print "Creating Database named as ddsassignment2"
        createDB();

        # Getting connection to the database
        print "Getting connection from the ddsassignment2 database"
        con = getOpenConnection()

        # Create Tables
        load_files_to_table(con)

        # Calling ParallelSort
        print "Performing Parallel Sort"
        ParallelSort(FIRST_TABLE_NAME, SORT_COLUMN_NAME_FIRST_TABLE, SORT_OUTPUT_TABLE_NAME, con);

        # Calling ParallelJoin
        print "Performing Parallel Join"
        ParallelJoin(FIRST_TABLE_NAME, SECOND_TABLE_NAME, JOIN_COLUMN_NAME_FIRST_TABLE, JOIN_COLUMN_NAME_SECOND_TABLE, JOIN_OUTPUT_TABLE_NAME, con);

        # Saving parallelSortOutputTable and parallelJoinOutputTable on two files
        saveTable(SORT_OUTPUT_TABLE_NAME, SORT_OUTPUT_TABLE_NAME+'.txt', con);
        saveTable(JOIN_OUTPUT_TABLE_NAME, JOIN_OUTPUT_TABLE_NAME+'.txt', con);

        # Deleting parallelSortOutputTable and parallelJoinOutputTable
        # deleteTables(SORT_OUTPUT_TABLE_NAME, con);
        #   	deleteTables(JOIN_OUTPUT_TABLE_NAME, con);

        if con:
            con.close()

    except Exception as detail:
        print "Something bad has happened!!! This is the error ==> ", detail
