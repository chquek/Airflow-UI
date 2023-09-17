import pprint
import logging

logger = logging.getLogger("kinisi")

def info ( *args , **context ) :
    task = context['dtp']
    print ("Form = " , task.form)
    print ("Task Instance" , context['ti'])
    print ("DAG params" , context['params'] )
    print ("TASK params" , task.form.task_params )
    for i in range(20) :
        try :
            print (f"DATA-{i} - {task.stream[i]}" )
        except :
            pass
    return None

def niceprint ( *args , **context ) :
    task = context['dtp']
    for streamid in range(20) :
        try :
            rows = task.stream[streamid]
            print ( f"DATA-{streamid}" )
            for row in rows :
                print (row)
        except :
            pass


# Used by ShortCircuit demo dag
def shortcircuit ( *args , **context ) :
    return dict(ans="hello world")
