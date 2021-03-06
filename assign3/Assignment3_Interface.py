#!/usr/bin/python2.7
#
# Assignment3 Interface
#

import psycopg2
import sys
from heapq import heappush, heappop, heapreplace, heapify
import threading

##########################################################################################################
########################################### Configurations ###############################################
##########################################################################################################
USER_ID_COLNAME = 'userid'
MOVIE_ID_COLNAME = 'movieid'
RATING_COLNAME = 'rating'
YEAR_ID_COLNAME = 'year'
COLLECTION_COLNAME = 'collection'

INTEGER_TYPE = 'INTEGER'
FLOAT_TYPE ='FLOAT'

SEPARATOR = '::'

RATINGS_TABLE = 'movieratings'
RATINGS_TABLE_COLUMNS = [USER_ID_COLNAME, MOVIE_ID_COLNAME, RATING_COLNAME]
RATINGS_TABLE_COLUMNS_TYPE = [INTEGER_TYPE, INTEGER_TYPE, FLOAT_TYPE]

COLLECTIONS_TABLE = 'moviecollections'
COLLECTIONS_TABLE_COLUMNS = [MOVIE_ID_COLNAME, YEAR_ID_COLNAME, COLLECTION_COLNAME]
COLLECTIONS_TABLE_COLUMNS_TYPE = [INTEGER_TYPE, INTEGER_TYPE, INTEGER_TYPE]

RATINGS_INPUT_FILE_PATH = RATINGS_TABLE+ '.dat'
COLLECTION_INPUT_FILE_PATH = COLLECTIONS_TABLE + '.dat'

SORT_OUTPUT_TABLE_NAME = 'parallelSortOutputTable'
JOIN_OUTPUT_TABLE_NAME = 'parallelJoinOutputTable'

PARALLEL_SORTING = True
PARALLEL_JOINING = True
NUM_OF_THREADS = 5

FIRST_TABLE_NAME = RATINGS_TABLE
SECOND_TABLE_NAME = COLLECTIONS_TABLE
SORT_COLUMN_NAME_FIRST_TABLE = RATING_COLNAME
SORT_COLUMN_NAME_SECOND_TABLE = COLLECTION_COLNAME
JOIN_COLUMN_NAME_FIRST_TABLE = MOVIE_ID_COLNAME
JOIN_COLUMN_NAME_SECOND_TABLE = MOVIE_ID_COLNAME


##########################################################################################################
#################################### Utility classes and functions #######################################
##########################################################################################################

''' a helper function to create test tables '''
def create_table(cur, table, columns, columns_type):
    cur.execute("DROP TABLE IF EXISTS " + table)
    createTableCmd = 'CREATE TABLE ' + table + '('
    size = len(columns)
    for i in range(size): createTableCmd += columns[i] + ' ' + columns_type[i] + (', ' if i < size-1 else '')
    createTableCmd += ')'
    print createTableCmd
    cur.execute(createTableCmd)


''' a helper function to create test tables '''
def insert_table(cur, table, columns, data):
    insertCmd = 'INSERT INTO ' + table + ' VALUES ('
    size = len(columns)
    for i in range(size): insertCmd += data[i] + (', ' if i < size-1 else '')
    insertCmd += ')'
    cur.execute(insertCmd)


''' a hard coded helper function to load two test files into tables'''
def load_files_to_table(openconnection):
    f = None
    cur = None
    try:
        cur = openconnection.cursor()

        # create test tables
        create_table(cur, RATINGS_TABLE, RATINGS_TABLE_COLUMNS, RATINGS_TABLE_COLUMNS_TYPE)
        create_table(cur, COLLECTIONS_TABLE, COLLECTIONS_TABLE_COLUMNS, COLLECTIONS_TABLE_COLUMNS_TYPE)

        # movie ratings table
        print 'reading records from ', RATINGS_INPUT_FILE_PATH
        f = open(RATINGS_INPUT_FILE_PATH, "r")

        line = f.readline()
        while line:
            insert_table(cur, RATINGS_TABLE, RATINGS_TABLE_COLUMNS, line.split(SEPARATOR))
            line = f.readline()

        # movie collections table
        print 'reading records from ', COLLECTION_INPUT_FILE_PATH
        f = open(COLLECTION_INPUT_FILE_PATH, "r")

        line = f.readline()
        while line:
            insert_table(cur, COLLECTIONS_TABLE, COLLECTIONS_TABLE_COLUMNS, line.split(SEPARATOR))
            line = f.readline()

        openconnection.commit()
    except Exception as detail:
        print 'load_files_to_table failed:', detail
    finally:
        if f: f.close()
        if cur: cur.close()


''' Thread used for parallel sorting '''
# in-place sorting, no need to return result
class sortingThread (threading.Thread):
    def __init__(self, arr, tarCol, name):
        threading.Thread.__init__(self)
        self.arr = arr
        self.tarCol = tarCol
        self.name = name

    def run(self):
        # print('thread ' + str(self.name) + ' started')
        self.arr.sort(key=lambda tup:tup[self.tarCol])
        # print('thread ' + str(self.name) + ' finished')


''' Thread used for parallel joining'''
class joiningThread (threading.Thread):
    def __init__(self, rows1, tarCol1, rows2, tarCol2, name):
        threading.Thread.__init__(self)
        self.rows1 = rows1
        self.tarCol1 = tarCol1
        self.rows2 = rows2
        self.tarCol2 = tarCol2
        self.name = name
        self.result=[]

    def run(self):
        # print('thread ' + str(self.name) + ' started')
        for row1 in self.rows1:
            for row2 in self.rows2:
                if row1[self.tarCol1] == row2[self.tarCol2]:
                    newrow = str(row1[self.tarCol1])
                    for idx, col in enumerate(row1):
                        if idx == self.tarCol1: continue
                        newrow += ',' + str(row1[idx])
                    for idx, col in enumerate(row2):
                        if idx == self.tarCol2: continue
                        newrow += ',' + str(row2[idx])
                    self.result.append(newrow)
        # print('thread ' + str(self.name) + ' finished')

    def getResult(self):
        return self.result

''' get the index of a column '''
def getColumnIndex(table, column, conn):
    cur = conn.cursor()
    sqlCmd = "select column_name, data_type from information_schema.columns where table_name = '" + table + "'"
    cur.execute(sqlCmd)
    rows = cur.fetchall()
    for idx, row in enumerate(rows):
        if row[0] == column:
            return idx
    return -1

''' get the definition of a column in the format of [column_name, data_type]'''
def getColumnDefinition(table, conn):
    cur = conn.cursor()
    sqlCmd = "select column_name, data_type from information_schema.columns where table_name = '" + table + "'"
    cur.execute(sqlCmd)
    rows = cur.fetchall()
    return rows

##########################################################################################################
#################################### Parallel Sorting and Joining  #######################################
##########################################################################################################

def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
    if not InputTable or not SortingColumnName or not OutputTable or not openconnection:
        return
    cur = None
    try:
        print('- - - - - - Start sorting ' + InputTable + ' on ' + SortingColumnName + ' - - - - - -')
        cur = openconnection.cursor()

        # get the index of the sorting column
        tarCol = getColumnIndex(InputTable, SortingColumnName, openconnection)
        # print('target column index = ' + str(tarCol))
        if (tarCol < 0):
            print('target column '+ SortingColumnName + ' not found!')
            return

        # divide all rows into N buckets
        cur.execute('SELECT * from ' + InputTable)
        rows = cur.fetchall()
        buckets = [[] for _ in range(NUM_OF_THREADS)]
        idx = 0
        for row in rows:
            buckets[idx].append(row)
            idx+=1
            if idx >= NUM_OF_THREADS: idx = 0

        # and sort each bucket in parallel with K threads
        if PARALLEL_SORTING:
            threads = []
            for i in range (NUM_OF_THREADS):
                t = sortingThread(buckets[i], tarCol, i)
                threads.append(t)
                t.start()
            # print(str(NUM_OF_THREADS)  + ' threads started')
            for i in range (NUM_OF_THREADS):
                threads[i].join()
        else:
            for i in range(NUM_OF_THREADS):
                buckets[i].sort(key=lambda tup:tup[tarCol])
                # print buckets[i]

        # create the output table
        cur.execute("DROP TABLE IF EXISTS " + OutputTable)
        createTableCmd = 'CREATE TABLE ' + OutputTable + ' AS SELECT * FROM ' + InputTable + ' WHERE ' + SortingColumnName + '!=' + SortingColumnName
        # print createTableCmd
        cur.execute(createTableCmd)

        # merge each sorted list to output to the table
        # using a binary min heap that each item is of the format (key, index, list)
        heap = [(b[0][tarCol], 0, b) for b in buckets if b]
        heapify(heap)
        while heap:
            tup = heap[0]
            index = tup[1]
            sortedlist = tup[2]

            item = sortedlist[index]
            # print(item)
            insertCmd = 'INSERT INTO ' + OutputTable + ' VALUES' + str(item)
            cur.execute(insertCmd)

            if index >= len(sortedlist)-1: # the list has been used up
                heappop(heap)
            else:
                heapreplace(heap, (sortedlist[index+1][tarCol], index+1, sortedlist))

        openconnection.commit()
    except Exception as detail:
        print 'ParallelSort failed:', detail
        openconnection.rollback()
    finally:
        if cur: cur.close


def ParallelJoin (InputTable1, InputTable2, JoinColumnTable1, JoinColumnTable2, OutputTable, openconnection):
    if not InputTable1 or not InputTable2 or not JoinColumnTable1 or not JoinColumnTable2 or not OutputTable or not openconnection:
        return
    cur = None

    try:
        print ('- - - - - - - Start joining ' + InputTable1 + ' on ' + JoinColumnTable1 + ' and ' + InputTable2 + ' on ' + JoinColumnTable2 + ' - - - - - -')
        cur = openconnection.cursor()

        # get the index of the sorting column
        tarCol1 = getColumnIndex(InputTable1, JoinColumnTable1, openconnection)
        tarCol2 = getColumnIndex(InputTable2, JoinColumnTable2, openconnection)
        if tarCol1 < 0 or tarCol2 < 0: return

        # get the columns definitions
        colDef1 =  getColumnDefinition(InputTable1, openconnection)
        colDef2 =  getColumnDefinition(InputTable2, openconnection)

        # create the output table
        cur.execute("DROP TABLE IF EXISTS " + OutputTable)
        createTableCmd = 'CREATE TABLE ' + OutputTable + ' ('
        columns = colDef1[tarCol1][0] + ' ' + colDef1[tarCol1][1]

        for idx, definition in enumerate(colDef1):
            if idx == tarCol1: continue
            columns += ',' + colDef1[idx][0] + ' ' + colDef1[idx][1]

        for idx, definition in enumerate(colDef2):
            if idx == tarCol2: continue
            columns += ',' + colDef2[idx][0] + ' ' + colDef2[idx][1]

        createTableCmd += columns + ' )'
        print(createTableCmd)
        cur.execute(createTableCmd)

        # divide all rows of the first table into N buckets
        cur.execute('SELECT * from ' + InputTable1)
        rows1 = cur.fetchall()
        cur.execute('SELECT * from ' + InputTable2)
        rows2 = cur.fetchall()

        buckets = [[] for _ in range(NUM_OF_THREADS)]
        idx = 0
        for row in rows1:
            buckets[idx].append(row)
            idx+=1
            if idx >= NUM_OF_THREADS:idx=0

        # join rows1 and rows2
        newrows = []
        if PARALLEL_JOINING:
            threads = []
            for i in range (NUM_OF_THREADS):
                t = joiningThread(buckets[i], tarCol1, rows2, tarCol2, i)
                threads.append(t)
                t.start()
            # print(str(NUM_OF_THREADS)  + ' threads started')
            for i in range (NUM_OF_THREADS):
                threads[i].join()
                newrows.extend(threads[i].getResult())
        else:
            # simplest O(n^2) matching
            # TODO: can achieve O(nlgn) by sorting first
            for row1 in rows1:
                for row2 in rows2:
                    if row1[tarCol1] == row2[tarCol2]:
                        newrow = str(row1[tarCol1])
                        for idx, col in enumerate(row1):
                            if idx == tarCol1: continue
                            newrow += ',' + str(row1[idx])
                        for idx, col in enumerate(row2):
                            if idx == tarCol2: continue
                            newrow += ',' + str(row2[idx])
                        newrows.append(newrow)


        # add new rows to the output table
        for row in newrows:
            insertCmd = 'INSERT INTO ' + OutputTable + ' VALUES (' + str(row) + ' )'
            # print (insertCmd)
            cur.execute(insertCmd)

        openconnection.commit()
        print
    except Exception as detail:
        print 'ParallelSort failed:', detail
        openconnection.rollback()
    finally:
        if cur: cur.close


##########################################################################################################
########################################### Driver Functions #############################################
##########################################################################################################

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
    try:
        cursor = openconnection.cursor()
        cursor.execute("Select * from %s" %(ratingstablename))
        data = cursor.fetchall()
        openFile = open(fileName, "w")
        for row in data:
            for d in row:
                openFile.write(`d`+",")
            openFile.write('\n')
        openFile.close()
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
        # ParallelSort(SECOND_TABLE_NAME, SORT_COLUMN_NAME_SECOND_TABLE, SORT_OUTPUT_TABLE_NAME, con);

        # Calling ParallelJoin
        print "Performing Parallel Join"
        ParallelJoin(FIRST_TABLE_NAME, SECOND_TABLE_NAME, JOIN_COLUMN_NAME_FIRST_TABLE, JOIN_COLUMN_NAME_SECOND_TABLE, JOIN_OUTPUT_TABLE_NAME, con);

        # Saving parallelSortOutputTable and parallelJoinOutputTable on two files
        saveTable(SORT_OUTPUT_TABLE_NAME, SORT_OUTPUT_TABLE_NAME+'.txt', con);
        saveTable(JOIN_OUTPUT_TABLE_NAME, JOIN_OUTPUT_TABLE_NAME+'.txt', con);

        # Deleting parallelSortOutputTable and parallelJoinOutputTable
        deleteTables(SORT_OUTPUT_TABLE_NAME, con);
        deleteTables(JOIN_OUTPUT_TABLE_NAME, con);

        if con:
            con.close()

    except Exception as detail:
        print "Something bad has happened!!! This is the error ==> ", detail
