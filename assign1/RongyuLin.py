#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2

DATABASE_NAME = 'dds_assign1'
RANGE_TABLE_PREFIX = 'range_part'
RROBIN_TABLE_PREFIX = 'rrobin_part'
USER_ID_COLNAME = 'userid'
MOVIE_ID_COLNAME = 'movieid'
RATING_COLNAME = 'rating'
partitionMarks = []
robinCount = -1

def getopenconnection(user='postgres', password='franklin', dbname='ratings'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadratings(ratingstablename, ratingsfilepath, openconnection):
    try:
        openconnection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE)
        # con = psycopg2.connect("dbname='" + ratingstablename + "' user='postgres' password='franklin' host='localhost'")
        con = openconnection
        cur = con.cursor()
        cur.execute("DROP TABLE IF EXISTS " + ratingstablename)

        sentence = "CREATE TABLE {0} ({1} INT, {2} INT, {3} FLOAT)".format(ratingstablename, USER_ID_COLNAME, MOVIE_ID_COLNAME, RATING_COLNAME)
        cur.execute(sentence)
        for line in open(ratingsfilepath, 'r'):
            item = line.rstrip();
            content = item.split('::')
            cur.execute('INSERT INTO ratings VALUES (%s, %s, %s)', (content[0], content[1], content[2]))
        con.commit()
        print('load complete')
    
    except Exception as detail:
        print "OOPS! This is an loading error ==> ", detail
        con.rollback()
    finally:
        cur.close()

def rangepartition(ratingstablename, numberofpartitions, openconnection):
    try:
        openconnection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE)
        #con = psycopg2.connect("dbname='" + DATABASE_NAME + "' user='postgres' password='franklin' host='localhost'")
        con = openconnection
        cur = con.cursor()
        for i in range(0, numberofpartitions):
            if (i == 0):
                partitionMarks.append(i)
            else:
                partitionMarks.append(partitionMarks[i-1] + (5.0 / numberofpartitions))
            #print partitionMarks[i]
        partitionMarks.append(5.0)
        for k in range(1, numberofpartitions+1):
            number = RANGE_TABLE_PREFIX + str(k)
            cur.execute('DROP TABLE IF EXISTS %s' %(number))
            sentence = "CREATE TABLE {0} ({1} INT, {2} INT, {3} FLOAT)".format(number, USER_ID_COLNAME, MOVIE_ID_COLNAME, RATING_COLNAME)
            cur.execute(sentence)
        cur.execute('SELECT * FROM {0}'.format(ratingstablename))
        for row in cur.fetchall():
            for j in range(0, len(partitionMarks)):
                if j == len(partitionMarks) - 1:
                    cur.execute("INSERT INTO {0}{1} VALUES ({2}, {3}, {4})".format(RANGE_TABLE_PREFIX, str(j), row[0], row[1], row[2]))
                elif (row[2] >= partitionMarks[j]) & (row[2] < partitionMarks[j+1]):
                    cur.execute("INSERT INTO {0}{1} VALUES ({2}, {3}, {4})".format(RANGE_TABLE_PREFIX, str(j+1), row[0], row[1], row[2]))
                    break;
            #cur.execute("SELECT * FROM %s WHERE Rating <= %s"%(number, 5))
            #for col in cur.fetchall():
                #print col
            #print("separate")
        con.commit()
        
    except Exception as detail:
        print "OOPS! This is the error ==> ", detail
        con.rollback()
    finally:
        cur.close()

def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    try:
        openconnection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE)
        #con = psycopg2.connect("dbname='" + DATABASE_NAME + "' user='postgres' password='franklin' host='localhost'")
        con = openconnection
        cur = con.cursor()
        cur2 = con.cursor()
        global robinCount
        for i in range(1, numberofpartitions+1):
            tableName = RROBIN_TABLE_PREFIX + str(i)
            cur.execute('DROP TABLE IF EXISTS %s' %(tableName))
            sentence = "CREATE TABLE {0} ({1} INT, {2} INT, {3} FLOAT)".format(tableName, USER_ID_COLNAME, MOVIE_ID_COLNAME, RATING_COLNAME)
            cur.execute(sentence)
        cur2.execute('SELECT * FROM %s' %(ratingstablename))
        rows = cur2.fetchall()
        j = 1
        for row in rows:
            insertTable = RROBIN_TABLE_PREFIX + str(j)
            cur.execute("INSERT INTO %s VALUES (%s, %s, %s)" % (insertTable, row[0], row[1], row[2]))
            if j == numberofpartitions:
                j = 1
            else:
                j = j + 1
            robinCount = j
            #cur2.execute('SELECT * FROM %s' %(ratingstablename))
            #row = cur2.fetchone()
            #cur.execute("SELECT * FROM %s" % (insertTable))
            #for col in cur.fetchall():
                #print col
            #print('separate')
        con.commit()
        #print robinCount
        
    except Exception as detail:
        print "OOPS! This is the error ==> ", detail
        con.rollback()
    finally:
        cur.close()

def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    try:
        #openconnection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE)
        #con = psycopg2.connect("dbname='" + DATABASE_NAME + "' user='postgres' password='franklin' host='localhost'")
        con = openconnection
        cur = con.cursor()
        global robinCount
        if robinCount == -1:
            robinCount = 1
        robinInsert = RROBIN_TABLE_PREFIX + str(robinCount)
        cur.execute('INSERT INTO %s VALUES (%s, %s, %s)' % (robinInsert, userid, itemid, rating))
        #print("Round Robin Result:")s
        #cur.execute("SELECT * FROM %s" % (robinInsert))
        #for col in cur.fetchall():
            #print col
        con.commit()
        
    except Exception as detail:
        print "OOPS! This is the error ==> ", detail
        con.rollback()
    finally:
        cur.close()


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    try:
        #openconnection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE)
        #con = psycopg2.connect("dbname='" + DATABASE_NAME + "' user='postgres' password='franklin' host='localhost'")
        con = openconnection
        cur = con.cursor()
        for i in range(0, len(partitionMarks)):
            if i == len(partitionMarks)-1:
                selectTable = RANGE_TABLE_PREFIX + str(i)
                cur.execute("INSERT INTO %s VALUES (%s, %s, %s);" % (selectTable, userid, itemid, rating))
            elif ((rating > partitionMarks[i]) & (rating <= partitionMarks[i+1])):
                selectTable = RANGE_TABLE_PREFIX + str(i+1)
                #print selectTable
                cur.execute("INSERT INTO %s VALUES (%s, %s, %s);" % (selectTable, userid, itemid, rating))
                break
                #print("Range Insert Result:")
                #cur.execute("SELECT * FROM %s"%(selectTable))
                #for col in cur.fetchall():
                    #print col
        con.commit()
                
    except Exception as detail:
        print "OOPS! This is the error ==> ", detail
        con.rollback()
    finally:
        cur.close()


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
        print 'A database named {0} already exists'.format(dbname)

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
            loadratings('test_data.dat', con)
            # ###################################################################################
            # Anything in this area will not be executed as I will call your functions directly
            # so please add whatever code you want to add in main, in the middleware functions provided "only"
            # ###################################################################################

            # Use this function to do any set up after I finish testing, if you want to
            after_test_script_ends_middleware(con, DATABASE_NAME)

    except Exception as detail:
        print "OOPS! This is the end rror ==> ", detail
