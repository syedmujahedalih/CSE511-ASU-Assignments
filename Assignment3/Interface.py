#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2

def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadRatings(ratingstablename, ratingsfilepath, openconnection):

    crs = openconnection.cursor()
    createRatings = "CREATE TABLE IF NOT EXISTS {DB} (UserID INT, movieID INT, Rating FLOAT)".format(
        DB=ratingstablename)
    crs.execute(createRatings)
    openconnection.commit()
    with open(ratingsfilepath, "r") as file:
        for row in file:
            [userId, movieId, rating, timestamp] = row.split("::")
            crs.execute('INSERT INTO %s VALUES (%s,%s,%s);' % (ratingstablename, userId, movieId, rating))
    openconnection.commit()


def rangePartition(ratingstablename, numberofpartitions, openconnection):

    conn = openconnection
    crs = conn.cursor()
    dbx = 'range_part'
    creation_query = "CREATE TABLE IF NOT EXISTS range_meta(partno INT, from_rat FLOAT, to_rat float)"
    crs.execute(creation_query)
    # conn.commit
    #print numberofpartitions
    #print ratingstablename

    for i in range(0, numberofpartitions):
        #print i
        f = i * float(5 / numberofpartitions)
        t = (i + 1) * float(5 / numberofpartitions)
        dname = dbx + str(i)
        # print dname, f, t
        createRange = "CREATE TABLE IF NOT EXISTS {db} (UserID INT, movieID INT, Rating FLOAT)".format(db=dname)
        crs.execute(createRange)
        conn.commit()
        if (i == 0):
            # test = "SELECT * from \"{r}\" ".format(r=ratingstablename)
            insertRange = "INSERT INTO {db} select * from {r}  where {r}.rating BETWEEN {f} AND {t}  ".format(db=dname,
                                                                                                              r=ratingstablename,
                                                                                                              f=f, t=t)
        else:
            insertRange = "INSERT INTO {db} select * from {r}  where {r}.rating > {f} AND {t} >= {r}.rating ".format(
                db=dname,
                r=ratingstablename,
                f=f, t=t)
        crs.execute(insertRange)
        conn.commit()
        insert_meta = "INSERT INTO range_meta VALUES ({partno},{f},{t})".format(partno=i, f=f, t=t)
        crs.execute(insert_meta)
        conn.commit()


def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):

    crs = openconnection.cursor()
    dbx = 'rrobin_part'
    creation_query = "CREATE TABLE IF NOT EXISTS rrobin_meta(partno INT, index INT)"
    crs.execute(creation_query)
    openconnection.commit()
    # print numberofpartitions
    # print ratingstablename
    sql_temp_create = "CREATE TABLE IF NOT EXISTS rrobin_temp (UserID INT, MovieID INT, Rating FLOAT, idx INT)"
    crs.execute(sql_temp_create)
    openconnection.commit()
    sql_temp_insert = "INSERT INTO rrobin_temp (SELECT {DB}.UserID, {DB}.MovieID, {DB}.Rating , (ROW_NUMBER() OVER() -1) % {n} as idx from {DB})".format(
        n=str(numberofpartitions), DB=ratingstablename)
    crs.execute(sql_temp_insert)
    openconnection.commit()
    for i in range(0, numberofpartitions):
        create_rrobin = "CREATE TABLE IF NOT EXISTS {DB} (UserID INT, MovieID INT, Rating FLOAT)".format(
            DB=dbx + str(i))
        crs.execute(create_rrobin)
        openconnection.commit()
        insert_rrobin = "INSERT INTO {DB} select userid,movieid,rating from rrobin_temp where idx = {idx}".format(
            DB=dbx + str(i), idx=str(i))
        crs.execute(insert_rrobin)
        openconnection.commit()

    sql_meta_insert = "INSERT INTO rrobin_meta SELECT {N} AS partno, count(*) % {N} from {DB}".format(
        DB=ratingstablename, N=numberofpartitions)
    crs.execute(sql_meta_insert)
    openconnection.commit()
    deleteTables('rrobin_temp', openconnection)
    openconnection.commit()


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):

    crs = openconnection.cursor()

    crs.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name LIKE 'rrobin_part%';")

    Number_of_partitions = int(crs.fetchone()[0])
    crs.execute("SELECT COUNT (*) FROM " + ratingstablename + ";")
    maxRows = int(crs.fetchone()[0])
    n = (maxRows) % Number_of_partitions

    crs.execute("INSERT INTO rrobin_part" + str(n) + " (UserID,MovieID,Rating) VALUES (" + str(userid) + "," + str(
        itemid) + "," + str(rating) + ");")

    crs.close()


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):

    conn = openconnection
    crs = conn.cursor()
    selection_query = "SELECT MIN(r.partno) FROM range_meta as r where r.from_rat <= {rat} and r.to_rat >= {rat} ".format(
        rat=rating)
    crs.execute(selection_query)
    conn.commit()
    partno = crs.fetchone()
    pno = partno[0]
    rate_insert = "Insert into {db} values ({u},{it},{r})".format(db=ratingstablename, u=userid, it=itemid, r=rating)
    crs.execute(rate_insert)
    conn.commit()
    range_insert = "Insert into range_part{i} values ({u},{it},{r})".format(i=pno, u=userid, it=itemid, r=rating)
    crs.execute(range_insert)
    conn.commit()


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
