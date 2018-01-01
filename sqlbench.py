from multiprocessing import Process, Lock, freeze_support, JoinableQueue, Queue, cpu_count
from multiprocessing.sharedctypes import Value, Array
from threading import Thread
import Queue as Q
import time
import sys
import random
import sys
import csv
import sqlite3
from scenarios import scenario1,scenario2

def consumer(name, start, control_queue, result_queue): 
    is_executing=False
    #random execution of time between scenarios
    time_between_scenarios=random.randint(1,5)
    scenarios={"scenario1":{"function":scenario1,"prefix":"scenario1_"},
        "scenario2":{"function":scenario2,"prefix":"scenario2_"}}
  
    results_list=[]
    while True:
        #print 'executing %s' % (name)
        #block until the controler says to start executing
        if not is_executing:
            next_ctrl_task = control_queue.get(True)
            if next_ctrl_task == "s":
                is_executing = True
        
        error=""
        exec_times=[]

        for scenario,sval in scenarios.iteritems():
            function = sval["function"] 
            prefix = sval["prefix"]

            t1=time.time()
            #time.sleep(0.1*random.randint(1, 3))
            try:
                exec_times=function(name,start,prefix)
            except Exception as e:
                error=str(e)

            t2=time.time()
            #it is better to send a complete list in the queue at the end to avoid blocking
            results_list.append((name,(t1-start),scenario, (t2-t1),error))
            #result_queue.put((name,(t1-start),"scenario1", (t2-t1),error))
            for exec_time in exec_times:
                results_list.append(exec_time)
            #result_queue.put(exec_time)
            time.sleep(time_between_scenarios)
     
        try:
            next_ctrl_task = control_queue.get(False)
            #Below is worse
            #to let time for other process
            #and give more chance to read the queue
            #next_ctrl_task = control_queue.get(True,time_between_task)
            if next_ctrl_task == "q":
                # Poison pill means shutdown
                #print '%s: Exiting' % name
                break
            elif next_ctrl_task == "s":
                #if we get "s" in this process, that means that we steal it from another process
                #so we pour it again
                control_queue.put("s")
            else:
                print("Control queue not empty and contains <%s>" % (next_ctrl_task))
        except Q.Empty:
            pass
        except Exception as e:
            print("Error %s" % (e))
        
        

    result_queue.put(results_list)

    return

def display_active_consumers(consumers,time_for_group):
    t1=time.time()
    while time.time()-t1 < time_for_group:
        active_consumers =len([x for x in consumers if x.is_alive()])
        print("active_consumers %d, "
            "execution will be terminated in %f s" % (active_consumers,
                (time_for_group-(time.time()-t1))))
        time.sleep(1)


def start_consumers(results_exec, start, nb_consumers, time_for_group):
    max_waiting_processes=60
    lock = Lock()

    controls = Queue()
    results = Queue()

    
    # Start  display_active_consumers thread
    consumers=[]

    # Start consumers
    num_consumers = nb_consumers

    print "Creating %d consumers" % num_consumers
    consumers = [ Process(target=consumer, args=(str(i), start, controls, results))
                  for i in xrange(num_consumers) ]
    
    for w in consumers:
        w.start()

    # Add a command to start each consumer
    for i in xrange(num_consumers):
        controls.put("s")

    time.sleep(1)

    print("Waiting max %d sec to processes to start executing" % (max_waiting_processes))
    tprocess = time.time()
    while (time.time()-tprocess) < max_waiting_processes:
        nbleft = controls.qsize()
        print("There are %d consumers not executing" % (nbleft))
        if nbleft == 0:
            break
        time.sleep(0.1)
    
    t = Thread(target=display_active_consumers, args=(consumers,time_for_group,))
    t.start()

    #to let time to processes to execute 
    print("Waiting %d sec to processes to execute" % (time_for_group))
    time.sleep(time_for_group)

    #only for main process
    #results.cancel_join_thread()

    # Add a poison pill for each consumer
    for i in xrange(num_consumers):
        controls.put("q")
    
    t.join()

    print("Waiting max %d sec to processes to terminate" % (max_waiting_processes))
    tprocess = time.time()
    while (time.time()-tprocess) < max_waiting_processes:
        nbleft = controls.qsize()
        print("There are %d consumers left" % (nbleft))
        if nbleft == 0:
            break
        time.sleep(0.1)

    len_results = 0
    print("Waiting max %d sec to let processes putting their data" % (max_waiting_processes))
    tprocess = time.time()
    while (time.time()-tprocess) < max_waiting_processes:
        len_results = results.qsize()
        print("There are %d results left" % (num_consumers - len_results))
        if (num_consumers - len_results) == 0:
            break
        time.sleep(0.1)

    len_results=results.qsize() #to not take None
    if len_results != num_consumers:
        print("Error: The number of results %d do not"
        " correspond to the number of consumers %d" % (len_results,num_consumers))
    
    iresult=0
    
    print("Reading %d tasks results progressively"
        " so processes can flush their data" % (len_results))
    #to finish the results, beacuse we let 10sec, we surely be the last putting data
    results.put(None)
    
    print("Wait 5s to be sure that the result queue is ready")
    time.sleep(5)

    for  results_list in iter(results.get, None):
        #read the result and wait 2ms, reduce the size of the queue and allow processes to flush
        #give also time to other process to execute 
        time.sleep(0.01)
        iresult +=1
        if (iresult % 10) == 0:
            print("Reading %d elements out of %d" % (iresult,len_results))
        for (name, start_exec, task_name, elapse_exec, error) in results_list:
            #uncomment to see intermediate result
            #print ("Result for %s consumers: Consumer %s, "
            #    "start_on %f, task %s, elapse_exec %f, error <%s>" % (num_consumers,
            #    name,start_exec,task_name, elapse_exec, error))
            results_exec.append((num_consumers, name, start_exec, task_name, 
                elapse_exec, error))

    #is_alive is false but processes are still there. 
    #These 2 lines insure that the processes finish. 
    #We use a timeout of 1 sec to not wait to long
    for c in consumers:
        c.join(1)

    print("Killing left process")
    #be sure that the consumers finish
    for c in consumers:
        if c.is_alive():
            c.terminate()
            c.join()
            
    print "End"

def tofdec(Nb):
    return str(Nb).replace(".",",")

def write_to_csv(results_exec):
    
    with open('results.csv', 'wb') as csvfile:
        results_writer = csv.writer(csvfile,delimiter=';',escapechar='"',
            quotechar='"',quoting=csv.QUOTE_NONNUMERIC)
        results_writer.writerow(["NumProc","Process", "StartOn", 
            "TaskName", "ElapseExec", "Error"])
        for line in results_exec:
            results_writer.writerow([line[0], line[1], 
                tofdec(line[2]), line[3], tofdec(line[4]),line[5]])

def delete_results_in_db():
    database = "results.db"
    conn = sqlite3.connect(database)
    c = conn.cursor()
    c.executescript("delete from results;")

def write_to_db(results_exec):
    database = "results.db"
 
    sql_create_results_table = """ CREATE TABLE IF NOT EXISTS results (
                                        id integer PRIMARY KEY AUTOINCREMENT,
                                        num_proc text NOT NULL,
                                        process text NOT NULL,
                                        start_on double NOT NULL,
                                        task_name text NOT NULL,
                                        elapse_exec double NOT NULL,
                                        error_desc text
                                    );
                                    
                                    CREATE VIEW IF NOT EXISTS results_scenario1 
                                    AS SELECT id,
                                    num_proc,
                                    process,
                                    start_on,
                                    task_name,
                                    elapse_exec,
                                    error_desc FROM results
                                    WHERE task_name='scenario1';
                                    
                                    CREATE VIEW IF NOT EXISTS results_scenario2 
                                    AS SELECT id,
                                    num_proc,
                                    process,
                                    start_on,
                                    task_name,
                                    elapse_exec,
                                    error_desc FROM results
                                    WHERE task_name='scenario2'; 
                                    
                                    CREATE VIEW IF NOT EXISTS results_aggr 
                                    AS SELECT num_proc,
                                    process,
                                    task_name,
                                    count(start_on) as start_on_count,
                                    avg(elapse_exec) as elapse_exec_avg,
                                    min(elapse_exec) as elapse_exec_min,
                                    max(elapse_exec) as elapse_exec_max FROM results
                                    GROUP BY num_proc,process,task_name;
                                    """

    conn = sqlite3.connect(database)
    c = conn.cursor()
    c.executescript(sql_create_results_table)
    with conn:
        sql = ''' INSERT INTO results(num_proc,process,start_on,
            task_name,elapse_exec,error_desc)
              VALUES(?,?,?,?,?,?) '''
        for line in results_exec:
            result=('%s'%line[0],'%s'%line[1],
                line[2],'%s'%line[3],line[4],'%s'%line[5])
            c.execute(sql, result)

if __name__ == '__main__':

    # Establish communication queues
    freeze_support()
    results_exec=[]

    start = time.time()
    
    #in sec
    time_for_group = 60
    
    #ramp
    for i in range(100,1000,100):
    #for i in range(200,300,100):
    #for i in range(10,30,10):
        #start at 100
        start_consumers(results_exec, start, i, time_for_group)
        time.sleep(1)

    results_exec = sorted(results_exec, key=lambda element: element[2])

    print "writing to csv file"
    write_to_csv(results_exec)

    print "writing to database"
    #delete_results_in_db()
    write_to_db(results_exec)


        
