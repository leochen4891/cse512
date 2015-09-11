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

POWER = 1
INPUT_FILE_PATH = 'test_data{0}.dat'.format(POWER)
ACTUAL_ROWS_IN_INPUT_FILE = 10**POWER  # Number of lines in the input file

def getopenconnection(user=USERNAME, password=PASSWORD, dbname=DATABASE_NAME):
    return psycopg2.connect("dbname='" + DATABASE_NAME + "' user='" + USERNAME + "' host='localhost' password='" + PASSWORD + "'")

def cleartable(tablename, openconnection):
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
        cleartable(ratingstablename, openconnection)

        cur = openconnection.cursor()

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

import math

def rangepartition(ratingstablename, numberofpartitions, openconnection):
    cur = None
    if not isinstance(numberofpartitions, int) or numberofpartitions <= 0:
        raise ValueError("number of partitions must be positive integer")

    try:
        cur = openconnection.cursor()
        cur.execute('SELECT * from {0}'.format(ratingstablename))
        rows = cur.fetchall()
        print 'size =', len(rows)
        part_size = len(rows) / numberofpartitions
        part_index = 1
        print 'part_size', part_size
        count = 0

        part_names = [RANGE_TABLE_PREFIX + str(i) for i in range(1,numberofpartitions+1)]

        for part_name in part_names:
            createTableCmd = 'CREATE TABLE {0}({1} INTEGER, {2} INTEGER, {3} FLOAT)'\
                    .format(part_name, USER_ID_COLNAME, MOVIE_ID_COLNAME, RATING_COLNAME)
            cur.execute(createTableCmd)
            openconnection.commit()

        for i, row in enumerate(rows):
            if count >= part_size and part_index < numberofpartitions:
                part_index +=1
                part_name = part_names[part_index-1]
                count = 0

            insertCmd = 'INSERT INTO {0} VALUES({1},{2},{3})'.format(part_name, *row)
            # print(insertCmd)
            cur.execute(insertCmd)
            count+=1

            # print i, part_name




        print 'committing'
        openconnection.commit()
        print 'committing finished'

    except Exception as detail:
        print 'range partition failed:', detail
    finally:
        if cur: cur.close()


def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    cur = None
    if not isinstance(numberofpartitions, int) or numberofpartitions <= 0:
        raise ValueError("number of partitions must be positive integer")

    try:
        cur = openconnection.cursor()
        cur.execute('SELECT * from {0}'.format(ratingstablename))
        rows = cur.fetchall()
        print 'size =', len(rows)
        robin = 0
        part_names = [RROBIN_TABLE_PREFIX + str(i) for i in range(1,numberofpartitions+1)]

        for part_name in part_names:
            createTableCmd = 'CREATE TABLE {0}({1} INTEGER, {2} INTEGER, {3} FLOAT)'\
                    .format(part_name, USER_ID_COLNAME, MOVIE_ID_COLNAME, RATING_COLNAME)
            cur.execute(createTableCmd)
            openconnection.commit()

        for row in rows:
            if robin >= numberofpartitions: robin = 0
            part_name = part_names[robin]
            insertCmd = 'INSERT INTO {0} VALUES({1},{2},{3})'.format(part_name, *row)
            # print(insertCmd)
            cur.execute(insertCmd)
            robin = (robin + 1)


            # print i, part_name




        print 'committing'
        openconnection.commit()
        print 'committing finished'

    except Exception as detail:
        print 'range partition failed:', detail
    finally:
        if cur: cur.close()


def deletepartitionsandexit(openconnection):
    cur = openconnection.cursor()

    for i in range(1,6):
        cur.execute("DROP TABLE IF EXISTS " + '{0}{1}'.format(RANGE_TABLE_PREFIX, i))
        cur.execute("DROP TABLE IF EXISTS " + '{0}{1}'.format(RROBIN_TABLE_PREFIX, i))
    openconnection.commit()
    cur.close


if __name__ == '__main__':
    con = None
    try:
        con = getopenconnection()

        deletepartitionsandexit(con)

        # loadratings(RATINGS_TABLE, INPUT_FILE_PATH, con)
        # rangepartition(RATINGS_TABLE, 3, con)
        # roundrobinpartition(RATINGS_TABLE, 3, con)


    except Exception as detail:
        print "OOPS! This is the error ==> ", detail

    finally:
        if con: con.close()
