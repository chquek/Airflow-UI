# templates/airflow.py #

import sys
import os
sys.path.append ("/home/kinisi")
sys.path.append ("/home/airflow")

from system.Task import Task

# customOperator.DAG derived from airflow.DAG
from airflow import DAG

from airflow import Dataset
from airflow.exceptions import AirflowSensorTimeout
import datetime
from datetime import datetime as dt2 , timedelta
from attrdict import AttrDict
import importlib
import logging
import json
# import pprint
# import pendulum
# import re
# from jinja2 import Environment, FileSystemLoader , Template

from airflow.utils.db import provide_session
from airflow.models import XCom
from udm.customOperator.Python import PythonOperator
from udm.customOperator.SimpleHttp import SimpleHttpOperator 

logger = logging.getLogger("kinisi")
DAG_ID = "RESTAPI"

tasks = [{'id': 356, 'pid': 1, 'name': 'HOOK-HTTP-GET', 'operator': 'HTTP', 'body': {'depend': [188], 'detail': {'baseop': 'from udm.customOperator.Python import PythonOperator', 'callback': {}, 'dataset': None, 'doc_md': None, 'python_callable': 'udm.HTTP.get', 'runnable': "PythonOperator ( task_id=taskname,  ** ( t.kwargs ( [ 'python_callable' ] ) ) ,  default_args=task_opargs , doc_md=doc_md , **callback , **dataset , **other_args , params=dagparams )", 'task_opargs': {}, 'task_params': {}, 'trigger': 'all_success', 'connid': 'kinisi-testflask', 'data': {'HttpHook_get': 'HttpHook_getvalue'}, 'endpoint': 'dummy', 'extra': None, 'headers': {'Content-Type': 'application/json', 'Authorization': 'Bearer access_token_goes_here'}, 'method': 'GET', 'other_args': {'trigger_rule': 'all_success'}}}, 'bfield': None}, {'id': 357, 'pid': 1, 'name': 'HOOK-HTTP-POST', 'operator': 'HTTP', 'body': {'depend': [188], 'detail': {'baseop': 'from udm.customOperator.Python import PythonOperator', 'callback': {}, 'dataset': None, 'doc_md': None, 'python_callable': 'udm.HTTP.post', 'runnable': "PythonOperator ( task_id=taskname,  ** ( t.kwargs ( [ 'python_callable' ] ) ) ,  default_args=task_opargs , doc_md=doc_md , **callback , **dataset , **other_args , params=dagparams )", 'task_opargs': {}, 'task_params': {}, 'trigger': 'all_success', 'connid': 'kinisi-testflask', 'data': {'HttpHook_post1': 'HttpHook_postvalue1', 'HttpHook_post2': 'HttpHook_postvalue2'}, 'endpoint': 'dummy', 'extra': None, 'headers': {'Content-Type': 'application/json', 'Authorization': 'Bearer access_token_goes_here'}, 'method': 'POST', 'other_args': {'trigger_rule': 'all_success'}}}, 'bfield': None}, {'id': 65, 'pid': 1, 'name': 'JSON-Reader', 'operator': 'Python', 'body': {'depend': [356, 357, 380, 381], 'detail': {'baseop': 'from udm.customOperator.Python import PythonOperator', 'callback': {}, 'dataset': None, 'doc_md': None, 'python_callable': 'udm.Template.info', 'runnable': "PythonOperator ( task_id=taskname,  ** ( t.kwargs ( [ 'python_callable' ] ) ) ,  default_args=task_opargs , doc_md=doc_md , **callback , **dataset , **other_args , params=dagparams )", 'task_opargs': {}, 'task_params': {}, 'trigger': 'all_success', 'other_args': {'trigger_rule': 'all_success'}}}, 'bfield': None}, {'id': 380, 'pid': 1, 'name': 'SimpleHttpOperator-GET', 'operator': 'SimpleHTTPOperator', 'body': {'depend': [188], 'detail': {'baseop': 'from udm.customOperator.SimpleHttp import SimpleHttpOperator ', 'callback': {}, 'data': {'SimpleHttpOperator_getkey': 'SimpleHttpOperator_getvalue'}, 'dataset': None, 'doc_md': '[see](https://airflow.readthedocs.io/en/1.9.0/_modules/http_operator.html)', 'endpoint': 'dummy', 'headers': {'Content-Type': 'application/json', 'Authorization': 'Bearer access_token_goes_here'}, 'http_conn_id': 'kinisi-testflask', 'method': 'GET', 'runnable': "SimpleHttpOperator( task_id=taskname, ** ( t.kwargs ( [ 'method' , 'http_conn_id' , 'endpoint' , 'headers' , 'data' ] ) ) , log_response = True ,  default_args=task_opargs , doc_md=doc_md , **callback , **dataset , **other_args , params=dagparams )", 'task_opargs': {}, 'task_params': {}, 'trigger': 'all_success', 'other_args': {'trigger_rule': 'all_success'}}}, 'bfield': None}, {'id': 381, 'pid': 1, 'name': 'SimpleHttpOperator-POST', 'operator': 'SimpleHTTPOperator', 'body': {'depend': [188], 'detail': {'baseop': 'from udm.customOperator.SimpleHttp import SimpleHttpOperator ', 'callback': {}, 'data': {'SimpleHttpOperator_postkey': 'SimpleHttpOperator_postvalue'}, 'dataset': None, 'doc_md': '[see](https://airflow.readthedocs.io/en/1.9.0/_modules/http_operator.html)', 'endpoint': 'dummy', 'headers': {'Content-Type': 'application/json', 'Authorization': 'Bearer access_token_goes_here'}, 'http_conn_id': 'kinisi-testflask', 'method': 'POST', 'runnable': "SimpleHttpOperator( task_id=taskname, ** ( t.kwargs ( [ 'method' , 'http_conn_id' , 'endpoint' , 'headers' , 'data' ] ) ) , log_response = True ,  default_args=task_opargs , doc_md=doc_md , **callback , **dataset , **other_args , params=dagparams )", 'task_opargs': {}, 'task_params': {}, 'trigger': 'all_success', 'other_args': {'trigger_rule': 'all_success'}}}, 'bfield': None}, {'id': 188, 'pid': 1, 'name': 'Start', 'operator': 'Python', 'body': {'depend': [], 'detail': {'baseop': 'from udm.customOperator.Python import PythonOperator', 'callback': {}, 'dataset': None, 'doc_md': None, 'python_callable': 'udm.Template.info', 'runnable': "PythonOperator ( task_id=taskname,  ** ( t.kwargs ( [ 'python_callable' ] ) ) ,  default_args=task_opargs , doc_md=doc_md , **callback , **dataset , **other_args , params=dagparams )", 'task_opargs': {}, 'task_params': {}, 'trigger': 'all_success', 'other_args': {'trigger_rule': 'all_success'}}}, 'bfield': None}]
dagparams = AttrDict({'dagparam': 123})

dag_kwargs = AttrDict ( {'description': 'Demo using HTTP for REST APIs', 'owner_links': AttrDict({}), 'concurrency': 8, 'catchup': True, 'tags': ['operator', 'other-uses'], 'doc_md': '# Header - DAG Markdown notes\n\n- See this for : [default arg options](https://airflow.apache.org/docs/apache-airflow/1.10.8/tutorial.html#default-arguments)\n', 'template_searchpath': '/opt/bash'}  )
default_args = AttrDict({'owner': 'anybody', 'start_date_old': '2023-07-08T16:21', 'start_date': '2023-09-10T00:00:00-04:00'})

'''
'''
dag_kwargs.owner_links = AttrDict({})
dag_kwargs.tags = ['operator', 'other-uses']

dag_callbacks = AttrDict({})
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
    except Exception as e :
        logger.error ( f"Error loading {module}/{funcstr} - discarding - {e}\n")

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

        t = Task(task, dagname=DAG_ID)
        t.lookup ( lookup )

        doc_md = t.markdown()
        callback = t.callback()
        dataset = t.datasets()
        taskname = t.name()
        task_opargs = t.task_opargs()
        other_args = t.get("other_args")

        if t.id() == 356 :
            stream[taskname] = PythonOperator ( task_id=taskname,  ** ( t.kwargs ( [ 'python_callable' ] ) ) ,  default_args=task_opargs , doc_md=doc_md , **callback , **dataset , **other_args , params=dagparams )
        if t.id() == 357 :
            stream[taskname] = PythonOperator ( task_id=taskname,  ** ( t.kwargs ( [ 'python_callable' ] ) ) ,  default_args=task_opargs , doc_md=doc_md , **callback , **dataset , **other_args , params=dagparams )
        if t.id() == 65 :
            stream[taskname] = PythonOperator ( task_id=taskname,  ** ( t.kwargs ( [ 'python_callable' ] ) ) ,  default_args=task_opargs , doc_md=doc_md , **callback , **dataset , **other_args , params=dagparams )
        if t.id() == 380 :
            stream[taskname] = SimpleHttpOperator( task_id=taskname, ** ( t.kwargs ( [ 'method' , 'http_conn_id' , 'endpoint' , 'headers' , 'data' ] ) ) , log_response = True ,  default_args=task_opargs , doc_md=doc_md , **callback , **dataset , **other_args , params=dagparams )
        if t.id() == 381 :
            stream[taskname] = SimpleHttpOperator( task_id=taskname, ** ( t.kwargs ( [ 'method' , 'http_conn_id' , 'endpoint' , 'headers' , 'data' ] ) ) , log_response = True ,  default_args=task_opargs , doc_md=doc_md , **callback , **dataset , **other_args , params=dagparams )
        if t.id() == 188 :
            stream[taskname] = PythonOperator ( task_id=taskname,  ** ( t.kwargs ( [ 'python_callable' ] ) ) ,  default_args=task_opargs , doc_md=doc_md , **callback , **dataset , **other_args , params=dagparams )

    ''' Establish dependencies for each task '''
    for idx, task in enumerate(tasks):
        t = Task(task)
        for depend in t.depend() :
            parent = lookup[depend]
            stream[t.name()].set_upstream(stream[parent])