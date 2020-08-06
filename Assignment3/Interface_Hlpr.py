#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2

def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    
    cur = openconnection.cursor()
    with open(ratingsfilepath,"r") as file:
        for line in file:
            [userId,movieId,rating,timestamp] = line.split("::")
            cur.execute('INSERT INTO %s VALUES (%s,%s,%s)'%(ratingstablename,userId,movieId,rating))
    cur.commit()


def rangePartition(ratingstablename, numberofpartitions, openconnection):
    cur = openconnection.cursor()
    
    r = 5.0/numberofpartitions #since ratings are out of 5
    partition_number=0
    lower_bound = 0
    
    while lower_bound<5.0:
        if lower_bound == 0:
            cur.execute("DROP TABLE IF EXISTS range_part"+str(partition_number))
            cur.execute("CREATE TABLE range_part"+str(partition_number)+ " AS SELECT * FROM "+ratingstablename+" WHERE Rating>="+str(lower_bound)+ " AND Rating<="+str(lower_bound+r)+";")
            lower_bound += r
            partition_number += 1
            
        else:
            cur.execute("DROP TABLE IF EXISTS range_part"+str(partition_number))
            cur.execute("CREATE TABLE range_part"+str(partition_number)+" AS SELECT * FROM "+ratingstablename+" WHERE Rating>"+str(lower_bound)+ " AND Rating<="+str(lower_bound+r)+";")
            lower_bound += r
            partition_number += 1
            
    cur.close()


def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    cur = openconnection.cursor()
    
    
    for j in range(numberofpartitions):
        cur.execute("DROP TABLE IF EXISTS rrobin_part"+str(j))
        cur.execute("CREATE TABLE rrobin_part"+str(j)+ " AS SELECT * FROM "+ratingstablename+" WHERE row_id % " + str(numberofpartitions) + " = " + str((j+1)%numberofpartitions))
        
    cur.close()


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    cur = openconnection.cursor()

    cur.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name LIKE 'rrobin_part%';")

    Number_of_partitions = int(cur.fetchone())
    cur.execute("SELECT COUNT (*) FROM "+ratingstablename+";")
    maxRows = int(cur.fetchone())
    n = (maxRows) % Number_of_partitions
    

    cur.execute("INSERT INTO rrobin_part"+str(n)+" (UserID,MovieID,Rating) VALUES ("+str(userid)+","+str(itemid)+","+str(rating)+")")

    
    cur.close()


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    

    cur = openconnection.cursor()
    cur.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name LIKE 'range_part%';")

    number = int(cur.fetchone()[0])
    l = []
    temp = 0
    for i in range(number):
        l.append(temp+(5.0/number))
        temp = 5.0/number

    partition_number = 0
    for j in l:
        if rating < j:
            break
        else:
            if(rating == j):
                break
            else:
                partition_number += 1
    
    cur.execute("INSERT INTO range_part"+str(partitionnumber)+" (UserID,MovieID,Rating) VALUES (%s, %s, %s)"%(userid, itemid, rating))

    cur.close()



def createDB(dbname='dds_assignment'):
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
    con.close()

def deletepartitionsandexit(openconnection):
    cur = openconnection.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    l = []
    for row in cur:
        l.append(row[0])
    for tablename in l:
        cur.execute("drop table if exists {0} CASCADE".format(tablename))

    cur.close()

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
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
    finally:
        if cursor:
            cursor.close()
