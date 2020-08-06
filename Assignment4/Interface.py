#!/usr/bin/python2.7
#
# Assignment2 Interface
#

#Mujahed_Syed

import psycopg2
import os
import sys
# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
    # print("printing query results")
    dname = str(os.path.dirname(__file__))
    total_lst = []
    curs = openconnection.cursor()
    range_name = "RangeRatingsPart"
    rrobin_name = "RoundRobinRatingsPart"
    #f = open('RangeQueryOut.txt', 'w')
    curs.execute("Select PartitionNum from RangeRatingsMetadata where " + str(ratingMinValue) + "<=minrating or " + str(
        ratingMaxValue) + ">=maxrating")
    num_range_parts = curs.fetchall()
    print (num_range_parts)
    lst1 = []

    for each in num_range_parts:
        tableName = range_name + str(each[0])
        curs.execute("Select * from " + tableName + " where Rating>=" + str(ratingMinValue) + " AND Rating <=" + str(
            ratingMaxValue))
        res = curs.fetchall()
        print(res)

        for each in res:
            lst1.append(tableName + "," + str(each[0]) + "," + str(each[1]) + "," + str(each[2]))
            #lst1.append(tableName + str(each[0]) + str(each[1]) + str(each[2]) + "\n")
        total_lst = total_lst + lst1

    #-----------
    #     for item in res:
    #         k = "RangeRatingsPart" + str(each[0]) + "," + ','.join(item)
    #         lst1.append(k)
    #     total_lst = total_lst + lst1
    # writeToFile(dname + '/' + 'RangeQueryOut.txt', total_lst)

    curs.execute("Select Partitionnum from RoundRobinRatingsMetadata")
    num_rrobin_parts = curs.fetchall()
    print("round robin parts:")
    print (num_rrobin_parts)
    lst2 = []

    for each in range(0, num_rrobin_parts[0][0]):
        tableName = rrobin_name + str(each)
        curs.execute("Select * from " + tableName + " where Rating>=" + str(ratingMinValue) + " AND Rating <=" + str(
            ratingMaxValue))
        r = curs.fetchall()

        for each in r:
            lst2.append(tableName + "," + str(each[0]) + "," + str(each[1]) + "," + str(each[2]))
        total_lst = total_lst + lst2
    writeToFile(dname + '/' + 'RangeQueryOut.txt', total_lst)



def PointQuery(ratingsTableName, ratingValue, openconnection):

    dname = str(os.path.dirname(__file__))
    total_lst = []
    curs = openconnection.cursor()
    # curs.execute("DROP TABLE IF EXISTS " + ratingstablename)
    range_name = "RangeRatingsPart"
    rrobin_name = "RoundRobinRatingsPart"

    curs.execute("Select PartitionNum from RangeRatingsMetadata where " + str(ratingValue) + ">=minrating and " + str(
        ratingValue) + "<=maxrating")
    num_range_parts = curs.fetchall()
    print (num_range_parts)

    lst1 = []

    for each in num_range_parts:
        tableName = range_name + str(each[0])
        curs.execute("Select * from " + tableName + " where Rating =" + str(ratingValue))
        res = curs.fetchall()
        for each in res:
            lst1.append(tableName + "," + str(each[0]) + "," + str(each[1]) + "," + str(each[2]))
        total_lst = total_lst + lst1

    curs.execute("Select Partitionnum from RoundRobinRatingsMetadata")
    num_rrobin_parts = curs.fetchall()
    print("point rrobin")
    print (num_rrobin_parts)

    lst2 = []

    for each in range(0, num_rrobin_parts[0][0]):
        tableName = rrobin_name + str(each)
        curs.execute("Select * from " + tableName + " where Rating =  " + str(ratingValue))
        r = curs.fetchall()
        for each in r:
            lst2.append(tableName + "," + str(each[0]) + "," + str(each[1]) + "," + str(each[2]))
        total_lst = total_lst + lst2
    writeToFile(dname + '/' + 'PointQueryOut.txt', total_lst)


def writeToFile(filename, rows):
    f = open(filename, 'w')
    for line in rows:
        f.write(''.join(str(s) for s in line))
        f.write('\n')
    f.close()
