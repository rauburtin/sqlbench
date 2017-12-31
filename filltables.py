import sqlite3
import datetime as dt

def main():
    database = "sqlite.db"
 
    sql_create_projects_table = """ CREATE TABLE IF NOT EXISTS projects (
                                        id integer PRIMARY KEY,
                                        name text NOT NULL,
                                        begin_date text,
                                        end_date text
                                    ); 
                                    delete from projects;"""
 
    sql_create_tasks_table = """CREATE TABLE IF NOT EXISTS tasks (
                                    id integer PRIMARY KEY,
                                    name text NOT NULL,
                                    priority integer,
                                    status_id integer NOT NULL,
                                    project_id integer NOT NULL,
                                    begin_date text NOT NULL,
                                    end_date text NOT NULL,
                                    FOREIGN KEY (project_id) REFERENCES projects (id)
                                );
                                create index project_id_idx on tasks(project_id);
                                delete from tasks;"""
 
    conn = sqlite3.connect(database)
    c = conn.cursor()
    c.executescript(sql_create_projects_table)
    c.executescript(sql_create_tasks_table)

    with conn:
        # populate db
        for i in range(1000000):
            dt1= dt.datetime.now()
            dt2= dt1 + dt.timedelta(days=10)
            project = ('project%05d'%i, dt1, dt2)
            sql = ''' INSERT INTO projects(name,begin_date,end_date)
                  VALUES(?,?,?) '''
            c.execute(sql, project)
            project_id= c.lastrowid
            dtt1 = dt1
            dtt2 = dt1 + dt.timedelta(days=10)
            for j in range(10):
                task_1 = ('task%05d%05d'%(i,j), 1, 1, project_id, dtt1, dtt2)
                dtt1 = dtt1 + dt.timedelta(days=10)
                dtt2 = dtt2 + dt.timedelta(days=10)
                sql = ''' INSERT INTO tasks(name,priority,status_id,project_id,begin_date,end_date)
                    VALUES(?,?,?,?,?,?) '''

                c.execute(sql, task_1)

    


if __name__ == '__main__':
    main()