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
DAG_ID = "ExceptionHandling"

tasks = [{'id': 126, 'pid': 38, 'name': 'Task1', 'operator': 'Python', 'body': {'depend': [], 'detail': {'baseop': 'from airflow.operators.python import PythonOperator', 'callback': {}, 'code': 'udm.DemoTask.showfailure', 'dataset': None, 'default_args': {}, 'description': None, 'doc_md': None, 'op_kwargs': {}, 'runnable': 'PythonOperator ( task_id=taskname, python_callable=t.run , params=params ,  default_args=taskargs , doc_md=doc_md , **callback , **dataset )'}}, 'bfield': None, 'position': '{"left": 169, "top": 81, "weight": 1}'}]
params = {}

dag_kwargs = AttrDict ( {'owner_links': AttrDict({}), 'concurrency': 8, 'catchup': False, 'doc_md': ''}  )
default_args = {'retries': 2, 'retry_delay': 'datetime.timedelta(minutes=5)', 'email': ['airflow@example.com'], 'email_on_failure': False, 'email_on_retry': False, 'owner': 'anybody', 'start_date_old': '2023-07-27T20:23', 'start_date': '2023-07-25T20:23'}
default_args['retry_delay'] = datetime.timedelta(minutes=5)

'''
'''
dag_kwargs.owner_links = AttrDict({})

dag_callbacks = AttrDict({'on_success_callback': 'udm.DagCallback.dag_success_callback', 'on_failure_callback': 'udm.DagCallback.dag_failure_callback', 'sla_miss_callback': None, 'on_retry_callback': None, 'on_execute_callback': None})
cleanup = True

# Schedule may be a Dataset
schedule = None

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

        if t.id() == 126 :
            logger.error ( callback )
            stream[taskname] = PythonOperator ( task_id=taskname, python_callable=t.run , params=params ,  default_args=taskargs , doc_md=doc_md , **callback , **dataset )

    ''' Establish dependencies for each task '''
    for idx, task in enumerate(tasks):
        t = Task(task)
        for depend in t.depend() :
            parent = lookup[depend]
            stream[t.name()].set_upstream(stream[parent])