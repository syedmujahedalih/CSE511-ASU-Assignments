#!/usr/bin/python2.7
#
# Assignment3 Interface
#

#Mujahed Syed
#CSE511_Spring2020


import psycopg2
import os
import sys
import threading

FIRST_TABLE_NAME = 'MovieRating'
SECOND_TABLE_NAME = 'MovieBoxOfficeCollection'
SORT_COLUMN_NAME_FIRST_TABLE = 'Rating'
SORT_COLUMN_NAME_SECOND_TABLE = 'Collection'
JOIN_COLUMN_NAME_FIRST_TABLE = 'MovieID'
JOIN_COLUMN_NAME_SECOND_TABLE = 'MovieID'
# Donot close the connection inside this file i.e. do not perform openconnection.close()

def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):

    try:
        # Getting cursor of openconnection
        cur = openconnection.cursor()

        # Getting the range and mininum value of range from function Range
        interval_sort, rangeMin = Range(InputTable, SortingColumnName, openconnection)

        # getting the schema of InputTable
        cur.execute(
            "SELECT COLUMN_NAME,DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='" + InputTable + "'")
        schema = cur.fetchall()

        # tables to store and sort range partitions
        for i in range(5):

            tableName = "range_part" + str(i)
            cur.execute("DROP TABLE IF EXISTS " + tableName + "")
            cur.execute("CREATE TABLE " + tableName + " (" + schema[0][0] + " " + schema[0][1] + ")")

            for d in range(1, len(schema)):
                cur.execute("ALTER TABLE " + tableName + " ADD COLUMN " + schema[d][0] + " " + schema[d][1] + ";")

        # create five threads
        thread = [0, 0, 0, 0, 0]
        for i in range(5):

            if i == 0:
                lowrValue = rangeMin
                upprValue = rangeMin + interval_sort
            else:
                lowrValue = upprValue
                upprValue = upprValue + interval_sort

            thread[i] = threading.Thread(target=range_insert_sort, args=(
            InputTable, SortingColumnName, i, lowrValue, upprValue, openconnection))

            thread[i].start()

        for p in range(0, 5):
            thread[i].join()

        # Combining all sorted partitions to OutputTable

        cur.execute("DROP TABLE IF EXISTS " + OutputTable + "")
        cur.execute("CREATE TABLE " + OutputTable + " (" + schema[0][0] + " " + schema[0][1] + ")")

        for i in range(1, len(schema)):
            cur.execute("ALTER TABLE " + OutputTable + " ADD COLUMN " + schema[i][0] + " " + schema[i][1] + ";")

        for i in range(5):
            query_str = "INSERT INTO " + OutputTable + " SELECT * FROM " + "range_part" + str(i) + ""
            cur.execute(query_str)

    except Exception as message:
        print "Exception :", message
        # Clean up
    finally:

        for i in range(5):
            tableName = "range_part" + str(i)
            cur.execute("DROP TABLE IF EXISTS " + tableName + "")
        cur.close()
    #Implement ParallelSort Here.
    #pass #Remove this once you are done with implementation


def Range(InputTable, SortingColumnName, openconnection):

    cur = openconnection.cursor()

    # Getting max and min value of SortingColumnName
    cur.execute("SELECT MIN(" + SortingColumnName + ") FROM " + InputTable + "")
    minVal = cur.fetchone()
    range_min_val = (float)(minVal[0])

    cur.execute("SELECT MAX(" + SortingColumnName + ") FROM " + InputTable + "")
    maxVal = cur.fetchone()
    range_max_val = (float)(maxVal[0])

    interval = (range_max_val - range_min_val) / 5
    return interval, range_min_val

# Inserting sorted value
def range_insert_sort(InputTable, SortingColumnName, index, min_val, max_val, openconnection):

    cur=openconnection.cursor()

    table_name = "range_part" + str(index)

    # Check for minimum value of column
    if index == 0:
        query_str = "INSERT INTO " + table_name + " SELECT * FROM " + InputTable + "  WHERE " + SortingColumnName + ">=" + str(min_val) + " AND " + SortingColumnName + " <= " + str(max_val) + " ORDER BY " + SortingColumnName + " ASC"
    else:
        query_str = "INSERT INTO " + table_name + " SELECT * FROM " + InputTable + "  WHERE " + SortingColumnName + ">" + str(min_val) + " AND " + SortingColumnName + " <= " + str(max_val) + " ORDER BY " + SortingColumnName + " ASC"

    cur.execute(query_str)
    cur.close()
    return


def MinMax(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, openconnection):
    # Getting cursor of openconnection
    cur = openconnection.cursor()

    # Gets maximum and min value of column
    cur.execute("SELECT MIN(" + Table1JoinColumn + ") FROM " + InputTable1 + "")
    minimum1 = cur.fetchone()
    min1 = float(minimum1[0])

    cur.execute("SELECT MIN(" + Table2JoinColumn + ") FROM " + InputTable2 + "")
    minimum2 = cur.fetchone()
    min2 = float(minimum2[0])

    cur.execute("SELECT MAX(" + Table1JoinColumn + ") FROM " + InputTable1 + "")
    maximum1 = cur.fetchone()
    max1 = float(maximum1[0])

    cur.execute("SELECT MAX(" + Table2JoinColumn + ") FROM " + InputTable2 + "")
    maximum2 = cur.fetchone()
    max2 = float(maximum2[0])

    if max1 > max2:
        rangeMax = max1
    else:
        rangeMax = max2

    if min1 > min2:
        rangeMin = min2
    else:
        rangeMin = min1

    interval = (rangeMax - rangeMin) / 5

    return interval, rangeMin


def OutputRangeTable(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, schema1, schema2, interval,
                     range_min_value, openconnection):
    cur = openconnection.cursor();

    for i in range(5):

        range_table1_name = "table1_range" + str(i)
        range_table2_name = "table2_range" + str(i)

        if i == 0:
            lowrVal = range_min_value
            upprVal = range_min_value + interval
        else:
            lowrVal = upprVal
            upprVal = upprVal + interval

        cur.execute("DROP TABLE IF EXISTS " + range_table1_name + ";")
        cur.execute("DROP TABLE IF EXISTS " + range_table2_name + ";")

        if i == 0:
            cur.execute(
                "CREATE TABLE " + range_table1_name + " AS SELECT * FROM " + InputTable1 + " WHERE (" + Table1JoinColumn + " >= " + str(
                    lowrVal) + ") AND (" + Table1JoinColumn + " <= " + str(upprVal) + ");")
            cur.execute(
                "CREATE TABLE " + range_table2_name + " AS SELECT * FROM " + InputTable2 + " WHERE (" + Table2JoinColumn + " >= " + str(
                    lowrVal) + ") AND (" + Table2JoinColumn + " <= " + str(upprVal) + ");")

        else:
            cur.execute(
                "CREATE TABLE " + range_table1_name + " AS SELECT * FROM " + InputTable1 + " WHERE (" + Table1JoinColumn + " > " + str(
                    lowrVal) + ") AND (" + Table1JoinColumn + " <= " + str(upprVal) + ");")
            cur.execute(
                "CREATE TABLE " + range_table2_name + " AS SELECT * FROM " + InputTable2 + " WHERE (" + Table2JoinColumn + " > " + str(
                    lowrVal) + ") AND (" + Table2JoinColumn + " <= " + str(upprVal) + ");")

        # Output range table
        output_range_table = "output_table" + str(i)

        cur.execute("DROP TABLE IF EXISTS " + output_range_table + "")
        cur.execute("CREATE TABLE " + output_range_table + " (" + schema1[0][0] + " " + schema2[0][1] + ")")

        for j in range(1, len(schema1)):
            cur.execute(
                "ALTER TABLE " + output_range_table + " ADD COLUMN " + schema1[j][0] + " " + schema1[j][1] + ";")

        for j in range(len(schema2)):
            cur.execute(
                "ALTER TABLE " + output_range_table + " ADD COLUMN " + schema2[j][0] + "1" + " " + schema2[j][1] + ";")


def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    try:
        # Initializing cursor of openconnection
        cur = openconnection.cursor()

        interval, range_min_value = MinMax(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, openconnection)

        # Getting schemas of input tables
        cur.execute(
            "SELECT COLUMN_NAME,DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='" + InputTable1 + "'")
        schema1 = cur.fetchall()
        cur.execute(
            "SELECT COLUMN_NAME,DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='" + InputTable2 + "'")
        schema2 = cur.fetchall()

        # output table
        cur.execute("DROP TABLE IF EXISTS " + OutputTable + "")
        cur.execute("CREATE TABLE " + OutputTable + " (" + schema1[0][0] + " " + schema2[0][1] + ")")

        for i in range(1, len(schema1)):
            cur.execute("ALTER TABLE " + OutputTable + " ADD COLUMN " + schema1[i][0] + " " + schema1[i][1] + ";")

        for i in range(len(schema2)):
            cur.execute("ALTER TABLE " + OutputTable + " ADD COLUMN " + schema2[i][0] + "1" + " " + schema2[i][1] + ";")

        # Calling the OuputRangeTable function for temporary output range table

        OutputRangeTable(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, schema1, schema2, interval,
                         range_min_value, openconnection)

        # Creats five threads
        thread = [0, 0, 0, 0, 0]

        for i in range(5):
            thread[i] = threading.Thread(target=range_insert_join,
                                         args=(Table1JoinColumn, Table2JoinColumn, openconnection, i))

            thread[i].start()

        for a in range(0, 5):
            thread[i].join()

        # Inserts in output table
        for i in range(5):
            cur.execute("INSERT INTO " + OutputTable + " SELECT * FROM output_table" + str(i))

    except Exception as detail:
        print "Exception in ParallelJoin is ==>>", detail

        # Clean up
    finally:
        for i in range(5):
            cur.execute("DROP TABLE IF EXISTS table1_range" + str(i))
            cur.execute("DROP TABLE IF EXISTS table2_range" + str(i))
            cur.execute("DROP TABLE IF EXISTS output_table" + str(i))

        cur.close()

        #Implement ParallelJoin Here.
    #pass # Remove this once you are done with implementation


def range_insert_join(Table1JoinColumn, Table2JoinColumn, openconnection, TempTableId):
    cur = openconnection.cursor()

    query_str = "INSERT INTO output_table" + str(TempTableId) + " SELECT * FROM table1_range" + str(
        TempTableId) + " INNER JOIN table2_range" + str(TempTableId) + " ON table1_range" + str(
        TempTableId) + "." + Table1JoinColumn + "=" + "table2_range" + str(TempTableId) + "." + Table2JoinColumn + ";"

    cur.execute(query_str)
    cur.close()
    return
