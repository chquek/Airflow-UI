# templates/airflow.py #

import sys
import os
sys.path.append ("/home/airflow/klib/system")

from Task import Task

from airflow import DAG
import datetime
from datetime import datetime as dt2
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
'''

DAG_ID = "Excel"

project = {'dag_id': 'Excel', 'default_args': {'retries': 2, 'retry_delay': 'datetime.timedelta(seconds=5)', 'email': ['airflow@example.com'], 'email_on_failure': False, 'email_on_retry': False, 'owner': 'anybody', 'start_date': '2023-6-18T17:4'}, 'params': '{ \n  "dagparam" : 123 \n}\n'}
tasks = [AttrDict({'id': 74, 'pid': 5, 'name': 'Reader', 'operator': 'Python', 'body': {'depend': [], 'detail': {'baseop': 'from airflow.operators.python import PythonOperator', 'callback': {}, 'code': 'klib.Template.info', 'command': None, 'descr': None, 'description': None, 'doc_md': None, 'filename': 'SampleData.xlsx', 'op_kwargs': '{}', 'runnable': 'PythonOperator ( task_id=taskname, python_callable=t.run , doc_md=t.doc_md , **callback )'}}, 'bfield': None, 'position': '{"left": 133, "top": 86, "weight": 1}'})]

params = { 
  "dagparam" : 123 
}


# if start_date has tz info, assign to dag_kwargs , otherwise it should be in default_args
dag_kwargs = AttrDict ( {'concurrency': 32, 'catchup': True, 'tags': "[ 'excel' ]", 'doc_md': '# Header - DAG Markdown notes\n\n- See this for : [default arg options](https://airflow.apache.org/docs/apache-airflow/1.10.8/tutorial.html#default-arguments)\n'}  )
default_args = {'retries': 2, 'retry_delay': 'datetime.timedelta(seconds=5)', 'email': ['airflow@example.com'], 'email_on_failure': False, 'email_on_retry': False, 'owner': 'anybody', 'start_date': '2023-6-18T17:4'}

'''
'''
dag_kwargs.tags = [ 'excel' ]


RAW=1
dag_callbacks = AttrDict({'on_success_callback': 'msql.dag_success_callback', 'on_failure_callback': 'msql.dag_failure_callback', 'sla_miss_callback': None, 'on_retry_callback': None, 'on_execute_callback': None})
cleanup = True
RAW=2

# end_date needs to be in datetime format
dt = 'end_date'
try :
    dag_kwargs[dt] = dt2.strptime( dag_kwargs[dt] , '%Y-%m-%dT%H:%M')
except :
    dag_kwargs[dt] = None

try :
    default_args['retry_delay'] = datetime.timedelta(seconds=5)
except :
    pass

# Runs the function pointer for X.Y ,  where Y is a function in X.py
def run_module ( funcstr , context ) :
    f = open ( f"/tmp/run.log" , "a") 
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
        taskname = t.name()

        if t.id() == 74 :
            stream[taskname] = PythonOperator ( task_id=taskname, python_callable=t.run , doc_md=t.doc_md , **callback )

    ''' Establish dependencies for each task '''
    for idx, task in enumerate(tasks):
        t = Task(task)
        for depend in t.depend() :
            parent = lookup[depend]
            stream[t.name()].set_upstream(stream[parent])