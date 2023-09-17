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
import re
from jinja2 import Environment, FileSystemLoader , Template
import logging

from airflow.utils.db import provide_session
from airflow.models import XCom
from airflow.operators.python import PythonOperator

logger = logging.getLogger()
DAG_ID = "RESTAPI"

tasks = [{'id': 65, 'pid': 1, 'name': 'JSON-Reader', 'operator': 'Python', 'body': {'depend': [52, 64], 'detail': {'baseop': 'from airflow.operators.python import PythonOperator', 'callback': {}, 'code': 'udm.Template.info', 'command': 'env', 'dataset': None, 'default_args': {}, 'descr': None, 'description': None, 'doc_md': None, 'op_kwargs': {}, 'runnable': "PythonOperator ( task_id=taskname, python_callable=t.run , op_kwargs = { 'additionalinfo' : 1 } ,  default_args=taskargs , doc_md=doc_md , **callback , **dataset )"}}, 'bfield': None, 'position': '{"left": 554, "top": 68, "weight": 1}'}, {'id': 52, 'pid': 1, 'name': 'REST-Caller-GET', 'operator': 'HTTP', 'body': {'depend': [], 'detail': {' endpoint': 'dummy', ' method': 'POST', 'NCtHbzKSCG': 'Secret', 'baseop': 'from airflow.operators.python import PythonOperator', 'callback': '{}', 'code': 'udm.HTTP.get', 'command': 'env', 'connid': 'TestFlask', 'data': {'get1': 'getvalue1'}, 'dataset': None, 'default_args': {}, 'descr': 'Demo a HTTP get', 'description': None, 'doc_md': None, 'endpoint': 'dummy', 'extra': None, 'headers': {'Content-Type': 'application/json', 'Authorization': 'Bearer access_token_goes_here'}, 'method': 'GET', 'op_kwargs': '{}', 'runnable': 'PythonOperator ( task_id=taskname, python_callable=t.run  ,  default_args=taskargs , doc_md=doc_md , **callback , **dataset )'}}, 'bfield': None, 'position': '{"left": 185, "top": 21, "weight": 1}'}, {'id': 64, 'pid': 1, 'name': 'REST-Caller-POST', 'operator': 'HTTP', 'body': {'depend': [], 'detail': {' endpoint': 'dummy', ' method': 'POST', 'NCtHbzKSCG': 'Secret', 'baseop': 'from airflow.operators.python import PythonOperator', 'callback': '{}', 'code': 'udm.HTTP.post', 'command': 'env', 'connid': 'TestFlask', 'data': {'post1': 'postvalue1', 'post2': 'postvalue2'}, 'dataset': None, 'default_args': {}, 'descr': 'Do a POST to  connid TestFlask', 'description': None, 'doc_md': '# Do a POST to  connid TestFlask', 'endpoint': 'dummy', 'extra': None, 'headers': {'Content-Type': 'application/json', 'Authorization': 'Bearer access_token_goes_here'}, 'method': 'POST', 'op_kwargs': '{}', 'runnable': 'PythonOperator ( task_id=taskname, python_callable=t.run ,  default_args=taskargs , doc_md=doc_md , **callback , **dataset )'}}, 'bfield': None, 'position': '{"left": 316, "top": 176, "weight": 1}'}]
params = { 
  "dagparam" : 123 
}


dag_kwargs = AttrDict ( {'description': 'Demo using HTTP for REST APIs', 'concurrency': 8, 'catchup': False, 'tags': ['http', 'schedule'], 'doc_md': '# Header - DAG Markdown notes\n\n- See this for : [default arg options](https://airflow.apache.org/docs/apache-airflow/1.10.8/tutorial.html#default-arguments)\n', 'template_searchpath': '/opt/bash'}  )
default_args = {'retries': 2, 'retry_delay': 'datetime.timedelta(seconds=5)', 'email': ['airflow@example.com'], 'email_on_failure': False, 'email_on_retry': False, 'owner': 'anybody', 'start_date_old': '2023-07-08T16:21', 'start_date': '2023-06-27T23:51:00-04:00'}
default_args['retry_delay'] = datetime.timedelta(seconds=5)

'''
'''
dag_kwargs.tags = ['http', 'schedule']

dag_callbacks = AttrDict({})
cleanup = True

# Schedule may be a Dataset

# Schedule is likely a string

# Schedule is timedelta or something similar ... expose as-is
schedule = timedelta(minutes=1)

dag_kwargs['schedule'] = schedule

# Runs the function pointer for X.Y ,  where Y is a function in X.py
def run_module ( funcstr , context ) :
    try :
        arr = funcstr.split('.')
        func = arr.pop()
        module = ".".join(arr)
        trun = importlib.import_module ( module )
        eval ( f"trun.{func}(context)" )
        logger.info ( f"Success - callback for {funcstr}")
    except Exception as e :
        logger.error ( f"Error - callback for {funcstr} - discarding - {e}\n")

@provide_session
def xcom_cleanup(session=None , **context) :
    try :
        session.query(XCom).filter(XCom.dag_id == DAG_ID).delete()
    except Exception as e :
        logger.error ( f"cleanup for {DAG_ID} failed : {e}" )


# if DAG , executed by scheduler while task executed by worker
# # so worker/scheduler has to mount workarea into /tmp
def success_callback(context) :
    if "on_success_callback" in dag_callbacks :
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

        doc_md = t.markdown()
        callback = t.callback()
        dataset = t.datasets()
        taskname = t.name()
        taskargs = t.default_args()

        if t.id() == 65 :
            logger.error ( callback )
            stream[taskname] = PythonOperator ( task_id=taskname, python_callable=t.run , op_kwargs = { 'additionalinfo' : 1 } ,  default_args=taskargs , doc_md=doc_md , **callback , **dataset )
        if t.id() == 52 :
            logger.error ( callback )
            stream[taskname] = PythonOperator ( task_id=taskname, python_callable=t.run  ,  default_args=taskargs , doc_md=doc_md , **callback , **dataset )
        if t.id() == 64 :
            logger.error ( callback )
            stream[taskname] = PythonOperator ( task_id=taskname, python_callable=t.run ,  default_args=taskargs , doc_md=doc_md , **callback , **dataset )

    ''' Establish dependencies for each task '''
    for idx, task in enumerate(tasks):
        t = Task(task)
        for depend in t.depend() :
            parent = lookup[depend]
            stream[t.name()].set_upstream(stream[parent])