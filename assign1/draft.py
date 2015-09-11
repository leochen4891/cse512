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

INPUT_FILE_PATH = 'test_data1.dat'
ACTUAL_ROWS_IN_INPUT_FILE = 100  # Number of lines in the input file



fo = open("foo.txt", "rw+")
print "Name of the file: ", fo.name

# Assuming file has following 5 lines
# This is 1st line
# This is 2nd line
# This is 3rd line
# This is 4th line
# This is 5th line

line = fo.readline()
print "Read Line: %s" % (line)

line = fo.readline(5)
print "Read Line: %s" % (line)

# Close opend file
fo.close()

con = None

try:

    con = psycopg2.connect("dbname='" + DATABASE_NAME + "' user='" + USERNAME + "' host='localhost' password='" + PASSWORD + "'")


    cur = con.cursor()

    # cur.execute("CREATE TABLE Cars(Id INTEGER PRIMARY KEY, Name VARCHAR(20), Price INT)")
    # cur.execute("INSERT INTO Cars VALUES(1,'Audi',52642)")
    # cur.execute("INSERT INTO Cars VALUES(2,'Mercedes',57127)")
    # cur.execute("INSERT INTO Cars VALUES(3,'Skoda',9000)")
    # cur.execute("INSERT INTO Cars VALUES(4,'Volvo',29000)")
    # cur.execute("INSERT INTO Cars VALUES(5,'Bentley',350000)")
    # cur.execute("INSERT INTO Cars VALUES(6,'Citroen',21000)")
    # cur.execute("INSERT INTO Cars VALUES(7,'Hummer',41400)")
    # cur.execute("INSERT INTO Cars VALUES(8,'Volkswagen',21600)")
    #
    # con.commit()

    cur.execute("SELECT * FROM Cars")

    rows = cur.fetchall()

    for row in rows:
        print row




    uId = 1
    uPrice = 60000

    cur.execute("UPDATE Cars SET Price=%s WHERE Id=%s", (uPrice, uId))

    cur.execute("SELECT * FROM Cars")
    row = cur.fetchone()
    while row:
        print row[0], row[1], row[2]
        row = cur.fetchone()


except psycopg2.DatabaseError, e:

    if con:
        con.rollback()

    print 'Error %s' % e
    exit(1)


finally:

    if con:
        con.close()