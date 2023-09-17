# templates/airflow.py #

import sys
import os
sys.path.append ("/home/kinisi")
sys.path.append ("/home/airflow")

from system.Task import Task

from airflow import DAG
from airflow import Dataset
import datetime
from datetime import datetime as dt2 , timedelta
from attrdict import AttrDict
import pprint
import json
import pendulum
import importlib

from airflow.utils.db import provide_session
from airflow.models import XCom
from airflow.operators.python import PythonOperator

'''
https://medium.com/analytics-vidhya/airflow-tricks-xcom-and-subdag-361ff5cd46ff
https://airflow.readthedocs.io/en/1.9.0/_modules/http_operator.html
project = {'dag_id': 'Callbacks', 'default_args': {'retries': 2, 'retry_delay': 'datetime.timedelta(seconds=5)', 'email': ['airflow@example.com'], 'email_on_failure': False, 'email_on_retry': False, 'owner': 'anybody', 'start_date_old': '2023-07-26T16:07', 'start_date': '2023-07-26T16:07'}, 'params': '{ \n  "dagparam" : 123 \n}\n'}
'''

DAG_ID = "Callbacks"

tasks = [AttrDict({'id': 122, 'pid': 36, 'name': 'Task1', 'operator': 'Python', 'body': {'depend': [], 'detail': {'baseop': 'from airflow.operators.python import PythonOperator', 'runnable': 'PythonOperator ( task_id=taskname, python_callable=t.run , doc_md=t.doc_md , params=params , **callback , **dataset )', 'code': 'udm.Template.info', 'dataset': None, 'description': None, 'doc_md': None, 'callback': {'on_success_callback': 'udm.Callback.task_success_callback', 'on_failure_callback': 'udm.Callback.on_failure_callback', 'on_retry_callback': 'udm.Callback.task_retry_callback', 'on_execute_callback': 'udm.Callback.task_execute_callback'}, 'op_kwargs': '{}'}}, 'bfield': None, 'position': '{"left": 50, "top": 50, "weight": 1}'})]

params = { 
  "dagparam" : 123 
}


dag_kwargs = AttrDict ( {'owner_links': AttrDict({}), 'concurrency': 8, 'catchup': False, 'doc_md': '# Header - DAG Markdown notes\n\n- See this for : [default arg options](https://airflow.apache.org/docs/apache-airflow/1.10.8/tutorial.html#default-arguments)\n'}  )
default_args = {'retries': 2, 'retry_delay': 'datetime.timedelta(seconds=5)', 'email': ['airflow@example.com'], 'email_on_failure': False, 'email_on_retry': False, 'owner': 'anybody', 'start_date_old': '2023-07-26T16:07', 'start_date': '2023-07-26T16:07'}

'''
'''
dag_kwargs.owner_links = AttrDict({})

dag_callbacks = AttrDict({'on_success_callback': 'udm.DagCallback.dag_success_callback', 'on_failure_callback': 'udm.DagCallback.dag_failure_callback', 'sla_miss_callback': None, 'on_retry_callback': None, 'on_execute_callback': None})
cleanup = True

try :
    default_args['retry_delay'] = datetime.timedelta(seconds=5)
except :
    pass

# Schedule may be a Dataset

# Schedule is likely a string

# Schedule is timedelta or something similar ... expose as-is

dag_kwargs['schedule'] = schedule

# Runs the function pointer for X.Y ,  where Y is a function in X.py
def run_module ( funcstr , context ) :
    try :
        arr = funcstr.split('.')
        func = arr.pop()
        module = ".".join(arr)
        trun = importlib.import_module ( module )
        eval ( f"trun.{func}(context)" )
        f.write ( f"Success - callback for {funcstr}\n")
    except Exception as e :
        f.write ( f"Error - callback for {funcstr} - discarding - {e}\n")
    f.close()

@provide_session
def xcom_cleanup(session=None , **context) :
    try :
        session.query(XCom).filter(XCom.dag_id == DAG_ID).delete()
    except Exception as e :
        fp = open ("/tmp/dag.log","a")
        fp.write (f"cleanup for {DAG_ID} failed : " , e)
        fp.close() 


# if DAG , executed by scheduler while task executed by worker
# # so worker/scheduler has to mount workarea into /tmp
def success_callback(context) :
    run_module ( dag_callbacks["on_success_callback"] , context )
    if cleanup == True :
        xcom_cleanup()

# log will appear in schedule pod
def failure_callback(context) :
    run_module ( dag_callbacks["on_failure_callback"] , context )

with DAG(DAG_ID, default_args=default_args, **dag_kwargs , on_success_callback = success_callback , on_failure_callback = failure_callback ) as dag:

    ''' Established all the dependencies first'''
    lookup = {}
    for idx, task in enumerate(tasks):
        t = Task(task)
        id = t.id()
        lookup[id] = t.name()


    # logic flow . this get the task/json/taskdetail , the stream will execute using the customPythonOperator.
    # This custom Operator invoke the method dynamic in classes/task.
    stream = {}
    for idx,task in enumerate(tasks) :

        t = Task(task)
        t.lookup ( lookup )

        try :
            t.doc_md = t.get('doc_md')
        except :
            t.doc_md = None

        callback = t.callback(DAG_ID)
        dataset = t.datasets()
        taskname = t.name()

        if t.id() == 122 :
            '''
            if t.get('dataset') :
                dataset = { 'outlets' : Dataset( t.get('dataset') ) }
            '''
            stream[taskname] = PythonOperator ( task_id=taskname, python_callable=t.run , doc_md=t.doc_md , params=params , **callback , **dataset )

    ''' Establish dependencies for each task '''
    for idx, task in enumerate(tasks):
        t = Task(task)
        for depend in t.depend() :
            parent = lookup[depend]
            stream[t.name()].set_upstream(stream[parent])