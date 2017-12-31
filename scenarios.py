import sqlite3
import time
import random

def scenario1(name, start, prefix):
    #time_between_task=0.1
    database = "sqlite.db"
    conn = sqlite3.connect(database)
    c = conn.cursor()

    exec_times=[]
    error=""

    def task1():
        sql1= """ select id, name, begin_date, end_date
            from projects
            where id = ? """
        sql2= """ select name, priority, status_id, project_id
            from tasks
            where project_id = ? """
        #print sql1
        project_id=random.randint(1,1000000-1)
        #print project_id

        t1=time.time()
        t = (project_id,)
        c.execute(sql1, t)
        result = c.fetchone()
        t2=time.time()

        exec_times.append((name,(t1-start),prefix+"task1_sql1", (t2-t1),error))

        t1=time.time()
        c.execute(sql2, t)
        result = c.fetchone()
        t2=time.time()

        exec_times.append((name,(t1-start),prefix+"task1_sql2", (t2-t1),error))
        #print "result", result

    def task2():
        sql3= """ select name, priority, status_id, project_id
            from tasks
            where project_id = ?  """
        sql4= """ select name, begin_date, end_date
            from projects
            where id = ? """

        project_id=random.randint(1,1000000-1)

        t1=time.time()
        t = (project_id,)
        for row in c.execute(sql3, t):
            result  = row
            #print 'row', row
        t2=time.time()

        exec_times.append((name,(t1-start),prefix+"task2_sql3", (t2-t1),error))

        t1=time.time()
        for j in range(10):
            t = (project_id,)
            c.execute(sql4, t)
            result = c.fetchone()
        t2=time.time()

        exec_times.append((name,(t1-start),prefix+"task2_10_sql4", (t2-t1),error))   

    for i in range(10):
        task1()
        task2()

    return exec_times


def scenario2(name, start, prefix):
    #time_between_task=0.1
    database = "sqlite.db"
    conn = sqlite3.connect(database)
    c = conn.cursor()

    exec_times=[]
    error=""

    def task1():
        sql1= """ select id, name, (end_date - begin_date) as diff_date
            from projects
            where id = ? """
        sql2= """ select distinct(priority)
            from tasks
            where project_id = ? """
        #print sql1
        project_id=random.randint(1,1000000-1)
        #print project_id

        t1=time.time()
        t = (project_id,)
        c.execute(sql1, t)
        result = c.fetchone()
        t2=time.time()

        exec_times.append((name,(t1-start),prefix+"task1_sql1", (t2-t1),error))

        t1=time.time()
        c.execute(sql2, t)
        result = c.fetchone()
        t2=time.time()

        exec_times.append((name,(t1-start),prefix+"task1_sql2", (t2-t1),error))
        #print "result", result

    def task2():
        sql3= """ select distinct(priority)
            from tasks
            where project_id = ?  """
        sql4= """ select name, (end_date - begin_date) as diff_date
            from projects
            where id = ? """

        project_id=random.randint(1,1000000-1)

        t1=time.time()
        t = (project_id,)
        for row in c.execute(sql3, t):
            result  = row
            #print 'row', row
        t2=time.time()

        exec_times.append((name,(t1-start),prefix+"task2_sql3", (t2-t1),error))

        t1=time.time()
        for j in range(10):
            t = (project_id,)
            c.execute(sql4, t)
            result = c.fetchone()
        t2=time.time()

        exec_times.append((name,(t1-start),prefix+"task2_10_sql4", (t2-t1),error))   

    for i in range(10):
        task1()
        task2()

    return exec_times