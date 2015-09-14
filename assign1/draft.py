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
numberofpartitions = 3
for numberofpartitions in range(1,12):
    rangeIndices = [min(numberofpartitions-1, int((float(d)/2)/(5.0/numberofpartitions))) for d in range(0,11)]
[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
[0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1]
[0, 0, 0, 0, 1, 1, 1, 2, 2, 2, 2]
[0, 0, 0, 1, 1, 2, 2, 2, 3, 3, 3]
[0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 4]
[0, 0, 1, 1, 2, 3, 3, 4, 4, 5, 5]
[0, 0, 1, 2, 2, 3, 4, 4, 5, 6, 6]
[0, 0, 1, 2, 3, 4, 4, 5, 6, 7, 7]
[0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 8]
[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 9]
[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]