# templates/airflow.py #

import sys
import os
sys.path.append ("/home/kinisi")
sys.path.append ("/home/airflow")

from system.Task import Task

from airflow import DAG
from airflow import Dataset
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
project = {'dag_id': 'APRYSE', 'default_args': {'retries': 2, 'retry_delay': 'datetime.timedelta(seconds=5)', 'email': ['airflow@example.com'], 'email_on_failure': False, 'email_on_retry': False, 'owner': 'anybody', 'start_date_old': '2023-07-08T18:01', 'start_date': '2023-07-05T18:01'}, 'params': '{ \n  "dagparam" : 123 \n}\n'}
'''

DAG_ID = "APRYSE"

tasks = [AttrDict({'id': 87, 'pid': 8, 'name': 'Merge', 'operator': 'PDF', 'body': {'depend': [86, 88], 'detail': {'baseop': 'from airflow.operators.python import PythonOperator', 'callback': {}, 'code': 'udm.PDF.merge', 'description': None, 'doc_md': None, 'filename': '/workarea/AAA.pdf', 'license': 'demo:1688081145858:7d92604403000000004098081f3b8141d4d261a141555a584492ac20c5', 'op_kwargs': '{}', 'runnable': 'PythonOperator ( task_id=taskname, python_callable=t.run , doc_md=t.doc_md , **callback , **callback , **dataset )'}}, 'bfield': None, 'position': '{"left": 613, "top": 80, "weight": 1}'}), AttrDict({'id': 86, 'pid': 8, 'name': 'Reader1', 'operator': 'PDF', 'body': {'depend': [89], 'detail': {'NCtHbzKSCG': 'Secret', 'baseop': 'from airflow.operators.python import PythonOperator', 'callback': {}, 'code': 'udm.PDF.readpages', 'description': None, 'doc_md': None, 'filename': '/workarea/interview2.pdf', 'license': 'demo:1688081145858:7d92604403000000004098081f3b8141d4d261a141555a584492ac20c5', 'op_kwargs': '{}', 'runnable': 'PythonOperator ( task_id=taskname, python_callable=t.run , doc_md=t.doc_md , **callback , **callback , **dataset )'}}, 'bfield': None, 'position': '{"left": 358, "top": 33, "weight": 1}'}), AttrDict({'id': 88, 'pid': 8, 'name': 'Reader2', 'operator': 'PDF', 'body': {'depend': [89], 'detail': {'NCtHbzKSCG': 'Secret', 'baseop': 'from airflow.operators.python import PythonOperator', 'callback': {}, 'code': 'udm.PDF.readpages', 'description': None, 'doc_md': None, 'filename': '/workarea/sample-pdf-download-10-mb.pdf', 'license': 'demo:1688081145858:7d92604403000000004098081f3b8141d4d261a141555a584492ac20c5', 'op_kwargs': '{}', 'runnable': 'PythonOperator ( task_id=taskname, python_callable=t.run , doc_md=t.doc_md , **callback , **callback , **dataset )'}}, 'bfield': None, 'position': '{"left": 356, "top": 190, "weight": 1}'}), AttrDict({'id': 89, 'pid': 8, 'name': 'Start', 'operator': 'Python', 'body': {'depend': [], 'detail': {'baseop': 'from airflow.operators.python import PythonOperator', 'callback': {}, 'code': 'udm.Template.info', 'description': None, 'doc_md': None, 'op_kwargs': '{}', 'runnable': 'PythonOperator ( task_id=taskname, python_callable=t.run , doc_md=t.doc_md , **callback , **callback , **dataset )'}}, 'bfield': None, 'position': '{"left": 139, "top": 99, "weight": 1}'})]

params = { 
  "dagparam" : 123 
}


dag_kwargs = AttrDict ( {'schedule': {'guess': 'string', 'value': '* * * * *'}, 'concurrency': 32, 'catchup': False, 'tags': ['pdf', 'merge', 'excel'], 'doc_md': '# Header - DAG Markdown notes\n\n- See this for : [default arg options](https://airflow.apache.org/docs/apache-airflow/1.10.8/tutorial.html#default-arguments)\n'}  )
default_args = {'retries': 2, 'retry_delay': 'datetime.timedelta(seconds=5)', 'email': ['airflow@example.com'], 'email_on_failure': False, 'email_on_retry': False, 'owner': 'anybody', 'start_date_old': '2023-07-08T18:01', 'start_date': '2023-07-05T18:01'}

'''
'''
dag_kwargs.tags = ['pdf', 'merge', 'excel']

dag_callbacks = AttrDict({'on_success_callback': 'msql.dag_success_callback', 'on_failure_callback': 'msql.dag_failure_callback', 'sla_miss_callback': None, 'on_retry_callback': None, 'on_execute_callback': None})
cleanup = True

try :
    default_args['retry_delay'] = datetime.timedelta(seconds=5)
except :
    pass

# Schedule may be a Dataset

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

        if t.id() == 87 :
            if t.get('dataset') :
                dataset = { 'outlets' : Dataset( t.get('dataset') ) }
            stream[taskname] = PythonOperator ( task_id=taskname, python_callable=t.run , doc_md=t.doc_md , **callback , **callback , **dataset )
        if t.id() == 86 :
            if t.get('dataset') :
                dataset = { 'outlets' : Dataset( t.get('dataset') ) }
            stream[taskname] = PythonOperator ( task_id=taskname, python_callable=t.run , doc_md=t.doc_md , **callback , **callback , **dataset )
        if t.id() == 88 :
            if t.get('dataset') :
                dataset = { 'outlets' : Dataset( t.get('dataset') ) }
            stream[taskname] = PythonOperator ( task_id=taskname, python_callable=t.run , doc_md=t.doc_md , **callback , **callback , **dataset )
        if t.id() == 89 :
            if t.get('dataset') :
                dataset = { 'outlets' : Dataset( t.get('dataset') ) }
            stream[taskname] = PythonOperator ( task_id=taskname, python_callable=t.run , doc_md=t.doc_md , **callback , **callback , **dataset )

    ''' Establish dependencies for each task '''
    for idx, task in enumerate(tasks):
        t = Task(task)
        for depend in t.depend() :
            parent = lookup[depend]
            stream[t.name()].set_upstream(stream[parent])