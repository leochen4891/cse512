__author__ = 'user'

import psycopg2
USERNAME = 'postgres'
PASSWORD = '1234'
DATABASE_NAME = 'dds_assgn1'

RATINGS_TABLE = 'ratings'
RANGE_TABLE_PREFIX = 'range_part'
RROBIN_TABLE_PREFIX = 'rrobin_part'

USER_ID_COLNAME = 'userid'
MOVIE_ID_COLNAME = 'movieid'
RATING_COLNAME = 'rating'
TIMESTAMP_COLNAME = 'timestamp'

POWER = 3
INPUT_FILE_PATH = 'test_data{0}.dat'.format(POWER)
ACTUAL_ROWS_IN_INPUT_FILE = 10**POWER  # Number of lines in the input file

def getopenconnection(user=USERNAME, password=PASSWORD, dbname=DATABASE_NAME):
    return psycopg2.connect("dbname='" + DATABASE_NAME + "' user='" + USERNAME + "' host='localhost' password='" + PASSWORD + "'")

def cleartable(user=USERNAME, password=PASSWORD, dbname=DATABASE_NAME):
    try:
        con = getopenconnection(user, password, dbname)
        cur = con.cursor()
        cur.execute("DROP TABLE IF EXISTS " + RATINGS_TABLE)

        # cur.execute("CREATE TABLE Cars(Id INTEGER PRIMARY KEY, Name VARCHAR(20), Price INT)")
        # cur.execute("INSERT INTO Cars VALUES(1,'Audi',52642)")
        createTableCmd = 'CREATE TABLE {0}({1} INTEGER, {2} INTEGER, {3} FLOAT, {4} INTEGER)'\
            .format(RATINGS_TABLE, USER_ID_COLNAME, MOVIE_ID_COLNAME, RATING_COLNAME, TIMESTAMP_COLNAME)
        # print createTableCmd
        cur.execute(createTableCmd)
        con.commit()
    finally:
        if con: con.close()


def loadratings(ratingstablename, ratingsfilepath, openconnection):
    f = None
    cur = None
    try:
        cur = con.cursor()

        print 'reading records from ', ratingsfilepath
        f = open(ratingsfilepath, "r")

        line = f.readline()
        while line:

            strs = line.split('::')

            # insertCmd = 'INSERT INTO {0} VALUES({1},{2},{3},{4})'.format(RATINGS_TABLE, strs[0], strs[1], strs[2],strs[3])
            insertCmd = 'INSERT INTO {0} VALUES({1},{2},{3},{4})'.format(RATINGS_TABLE, *strs)
            # print insertCmd
            cur.execute(insertCmd)
            line = f.readline()

        print 'reading finished'

        print 'committing'
        con.commit()
        print 'committing finished'

    except Exception as detail:
        print 'Read input file:', ratingsfilepath, 'failed:', detail
    finally:
        if f: f.close()
        if cur: cur.close()


if __name__ == '__main__':
    con = None
    try:
        cleartable()

        con = getopenconnection()

        loadratings(RATINGS_TABLE, INPUT_FILE_PATH, con)


    except Exception as detail:
        print "OOPS! This is the error ==> ", detail

    finally:
        if con: con.close()
