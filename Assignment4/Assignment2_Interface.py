#!/usr/bin/python2.7
#
# Assignment2 Interface
#

import psycopg2
import os
import sys
# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingMinValue, ratingMaxValue, openconnection, outputPath):
    #Implement RangeQuery Here.
	name = "roundrobinratingspart"
	#print("printing query results")
	cursor = openconnection.cursor()

	if ratingMaxValue > 5.0 or ratingMinValue < 0.0 or ratingMinValue > ratingMaxValue:
		print("Rating Parameters out of scope")
		exit(0)

	cursor.execute("select count(table_name) from information_schema.tables where table_name like 'roundrobinratingspart%' ;")
	numberofpartitions  = int(cursor.fetchall()[0][0])

	curtableno=0
	f = open(outputPath, "w+")
	while curtableno<numberofpartitions:

		curtable = name+str(curtableno)
		cursor.execute("select * from %s where rating >= %s and rating <= %s"%(curtable,ratingMinValue,ratingMaxValue))
		res=cursor.fetchall()
		for i in res:
			#print(i, curtable)
			f.write("RoundRobinRatingsPart%s,%s,%s,%s\n" % (curtableno, i[0], i[1], i[2]))
		curtableno+=1

	cursor.execute("select count(table_name) from information_schema.tables where table_name like 'rangeratingspart%' ;")

	name = "rangeratingspart"

	mn = 0.0
	mx = 5.0
	step = (mx - mn) / (float)(numberofpartitions)
	ub=step
	lb=0.0
	curtableno=0
	#f = open(outputPath, "w+")
	while (curtableno < numberofpartitions):
		curtable=name+str(curtableno)
		if (ratingMinValue <= ub):
			cursor.execute("select * from  %s where rating >= %s and rating <=  %s"%(curtable,ratingMinValue,ratingMaxValue))
			res=cursor.fetchall()
			for i in res:
				#print(i,curtable)
				f.write("RangeRatingsPart%s,%s,%s,%s\n"%(curtableno,i[0],i[1],i[2]))
		lb+=step
		ub+=step
		curtableno+=1
		if ratingMaxValue <= ub-step:
			break
	f.close()


	
			


def PointQuery(ratingValue, openconnection, outputPath):
	name = "roundrobinratingspart"
	#print("printing query results")
	cursor = openconnection.cursor()

	if ratingValue > 5.0 or ratingValue <0.0:
		print("Rating Parameters out of scope")
		exit(0)

	cursor.execute("select count(table_name) from information_schema.tables where table_name like 'roundrobinratingspart%' ;")
	numberofpartitions = int(cursor.fetchall()[0][0])

	curtableno = 0
	f = open(outputPath, "w+")
	while curtableno < numberofpartitions:

		curtable = name + str(curtableno)
		cursor.execute("select * from %s where rating = %s" % (curtable, ratingValue))
		res = cursor.fetchall()
		for i in res:
			#print(i, curtable)
			f.write("RoundRobinRatingsPart%s,%s,%s,%s\n" % (curtableno, i[0], i[1], i[2]))
		curtableno += 1

	name = "rangeratingspart"
	cursor.execute("select count(table_name) from information_schema.tables where table_name like 'rangeratingspart%' ;")
	numberofpartitions = int(cursor.fetchall()[0][0])
	mn = 0.0
	mx = 5.0
	step = (mx - mn) / (float)(numberofpartitions)
	ub = step
	lb = 0.0
	curtableno = 0

	while(curtableno < numberofpartitions):
		if lb==0.0:
			if ratingValue >=lb and ratingValue<=ub:
				break

		else:
			if ratingValue > lb and ratingValue <= ub:
				break
		lb+=step
		ub+=step
		curtableno+=1

	curtable=name+str(curtableno)
	cursor.execute("select * from %s where rating = %s" % (curtable, ratingValue))
	res = cursor.fetchall()
	for i in res:
		#print(i, curtable)
		f.write("RangeRatingsPart%s,%s,%s,%s\n" % (curtableno, i[0], i[1], i[2]))
	f.close()




