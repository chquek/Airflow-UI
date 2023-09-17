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
from udm.customOperator.BranchDateTime import BranchDateTimeOperator
from udm.customOperator.Python import PythonOperator
from airflow.operators.bash import BashOperator

logger = logging.getLogger("kinisi")
DAG_ID = "DateTimeBranching"

tasks = [{'id': 389, 'pid': 92, 'name': 'DT-Branching', 'operator': 'BranchDateTime', 'body': {'depend': [394], 'detail': {'baseop': 'from udm.customOperator.BranchDateTime import BranchDateTimeOperator', 'callback': {}, 'dataset': None, 'doc_md': None, 'follow_task_ids_if_false': 'OutOfRange_1 , OutOfRange_2', 'follow_task_ids_if_true': 'InRange_1 , InRange_2', 'otherval': 'Othervalue', 'runnable': "BranchDateTimeOperator( task_id=taskname,  ** ( t.kwargs ( [ 'follow_task_ids_if_true' , 'follow_task_ids_if_false' , 'target_lower' , 'target_upper' , 'use_task_logical_date' , 'use_task_execution_date' ] ) ) ,  default_args=task_opargs , doc_md=doc_md , **callback , **dataset , **other_args , params=dagparams )", 'target_lower': '2022-05-21 22:05', 'target_upper': '2024-05-21 22:05', 'task_opargs': {'retries': 5, 'retry_delay': 'timedelta(seconds=5)'}, 'task_params': {'key1': 'value1', 'key2': 'value2'}, 'trigger': 'all_success', 'use_task_logical_date ': 'False', 'other_args': {'trigger_rule': 'all_success'}}}, 'bfield': None}, {'id': 394, 'pid': 92, 'name': 'Genrows', 'operator': 'Python', 'body': {'depend': [388], 'detail': {'baseop': 'from udm.customOperator.Python import PythonOperator', 'callback': {}, 'dataset': None, 'doc_md': None, 'python_callable': 'udm.Generator.cars.newmodels', 'runnable': "PythonOperator ( task_id=taskname,  ** ( t.kwargs ( [ 'python_callable' ] ) ) ,  default_args=task_opargs , doc_md=doc_md , **callback , **dataset , **other_args , params=dagparams )", 'task_opargs': {}, 'task_params': {}, 'trigger': 'all_success', 'other_args': {'trigger_rule': 'all_success'}}}, 'bfield': None}, {'id': 390, 'pid': 92, 'name': 'InRange_1', 'operator': 'Python', 'body': {'depend': [389], 'detail': {'baseop': 'from udm.customOperator.Python import PythonOperator', 'callback': {}, 'dataset': None, 'doc_md': None, 'python_callable': 'udm.Template.info', 'runnable': "PythonOperator ( task_id=taskname,  ** ( t.kwargs ( [ 'python_callable' ] ) ) ,  default_args=task_opargs , doc_md=doc_md , **callback , **dataset , **other_args , params=dagparams )", 'task_opargs': {}, 'task_params': {}, 'trigger': 'all_success', 'other_args': {'trigger_rule': 'all_success'}}}, 'bfield': None}, {'id': 391, 'pid': 92, 'name': 'InRange_2', 'operator': 'Python', 'body': {'depend': [389], 'detail': {'baseop': 'from udm.customOperator.Python import PythonOperator', 'callback': {}, 'dataset': None, 'doc_md': None, 'python_callable': 'udm.Template.info', 'runnable': "PythonOperator ( task_id=taskname,  ** ( t.kwargs ( [ 'python_callable' ] ) ) ,  default_args=task_opargs , doc_md=doc_md , **callback , **dataset , **other_args , params=dagparams )", 'task_opargs': {}, 'task_params': {}, 'trigger': 'all_success', 'other_args': {'trigger_rule': 'all_success'}}}, 'bfield': None}, {'id': 392, 'pid': 92, 'name': 'OutOfRange_1', 'operator': 'Python', 'body': {'depend': [389], 'detail': {'baseop': 'from udm.customOperator.Python import PythonOperator', 'callback': {}, 'dataset': None, 'doc_md': None, 'python_callable': 'udm.Template.info', 'runnable': "PythonOperator ( task_id=taskname,  ** ( t.kwargs ( [ 'python_callable' ] ) ) ,  default_args=task_opargs , doc_md=doc_md , **callback , **dataset , **other_args , params=dagparams )", 'task_opargs': {}, 'task_params': {}, 'trigger': 'all_success', 'other_args': {'trigger_rule': 'all_success'}}}, 'bfield': None}, {'id': 393, 'pid': 92, 'name': 'OutOfRange_2', 'operator': 'Python', 'body': {'depend': [389], 'detail': {'baseop': 'from udm.customOperator.Python import PythonOperator', 'callback': {}, 'dataset': None, 'doc_md': None, 'python_callable': 'udm.Template.info', 'runnable': "PythonOperator ( task_id=taskname,  ** ( t.kwargs ( [ 'python_callable' ] ) ) ,  default_args=task_opargs , doc_md=doc_md , **callback , **dataset , **other_args , params=dagparams )", 'task_opargs': {}, 'task_params': {}, 'trigger': 'all_success', 'other_args': {'trigger_rule': 'all_success'}}}, 'bfield': None}, {'id': 388, 'pid': 92, 'name': 'Pip', 'operator': 'Bash', 'body': {'depend': [], 'detail': {'baseop': 'from airflow.operators.bash import BashOperator', 'bash_command': 'pip install pendulum', 'callback': {}, 'dataset': None, 'doc_md': None, 'runnable': "BashOperator ( task_id=taskname, ** ( t.kwargs ( [ 'bash_command' ] ) ) ,  default_args=task_opargs , doc_md=doc_md , **callback , **dataset , **other_args , params=dagparams )", 'task_opargs': {}, 'task_params': {}, 'trigger': 'all_success', 'other_args': {'trigger_rule': 'all_success'}}}, 'bfield': None}, {'id': 398, 'pid': 92, 'name': 'ReadGenRowsData', 'operator': 'Python', 'body': {'depend': [391], 'detail': {'baseop': 'from udm.customOperator.Python import PythonOperator', 'callback': {}, 'dataset': None, 'doc_md': None, 'python_callable': 'udm.DemoTask.readstream', 'runnable': "PythonOperator ( task_id=taskname,  ** ( t.kwargs ( [ 'python_callable' ] ) ) ,  default_args=task_opargs , doc_md=doc_md , **callback , **dataset , **other_args , params=dagparams )", 'task_opargs': {}, 'task_params': {'stage': 'Genrows'}, 'trigger': 'all_success', 'other_args': {'trigger_rule': 'all_success'}}}, 'bfield': None}]
dagparams = AttrDict({'home': '13grovewood'})

dag_kwargs = AttrDict ( {'owner_links': AttrDict({}), 'concurrency': 8, 'catchup': True, 'tags': ['operator'], 'doc_md': '# Header - DAG Markdown notes\n\n- See this for : [default arg options](https://airflow.apache.org/docs/apache-airflow/1.10.8/tutorial.html#default-arguments)\n'}  )
default_args = AttrDict({'owner': 'anybody', 'start_date_old': '2023-09-02T11:12', 'start_date': '2023-09-10T00:00'})

'''
'''
dag_kwargs.owner_links = AttrDict({})
dag_kwargs.tags = ['operator']

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

        if t.id() == 389 :
            task_opargs['retry_delay'] = timedelta(seconds=5)
            stream[taskname] = BranchDateTimeOperator( task_id=taskname,  ** ( t.kwargs ( [ 'follow_task_ids_if_true' , 'follow_task_ids_if_false' , 'target_lower' , 'target_upper' , 'use_task_logical_date' , 'use_task_execution_date' ] ) ) ,  default_args=task_opargs , doc_md=doc_md , **callback , **dataset , **other_args , params=dagparams )
        if t.id() == 394 :
            stream[taskname] = PythonOperator ( task_id=taskname,  ** ( t.kwargs ( [ 'python_callable' ] ) ) ,  default_args=task_opargs , doc_md=doc_md , **callback , **dataset , **other_args , params=dagparams )
        if t.id() == 390 :
            stream[taskname] = PythonOperator ( task_id=taskname,  ** ( t.kwargs ( [ 'python_callable' ] ) ) ,  default_args=task_opargs , doc_md=doc_md , **callback , **dataset , **other_args , params=dagparams )
        if t.id() == 391 :
            stream[taskname] = PythonOperator ( task_id=taskname,  ** ( t.kwargs ( [ 'python_callable' ] ) ) ,  default_args=task_opargs , doc_md=doc_md , **callback , **dataset , **other_args , params=dagparams )
        if t.id() == 392 :
            stream[taskname] = PythonOperator ( task_id=taskname,  ** ( t.kwargs ( [ 'python_callable' ] ) ) ,  default_args=task_opargs , doc_md=doc_md , **callback , **dataset , **other_args , params=dagparams )
        if t.id() == 393 :
            stream[taskname] = PythonOperator ( task_id=taskname,  ** ( t.kwargs ( [ 'python_callable' ] ) ) ,  default_args=task_opargs , doc_md=doc_md , **callback , **dataset , **other_args , params=dagparams )
        if t.id() == 388 :
            stream[taskname] = BashOperator ( task_id=taskname, ** ( t.kwargs ( [ 'bash_command' ] ) ) ,  default_args=task_opargs , doc_md=doc_md , **callback , **dataset , **other_args , params=dagparams )
        if t.id() == 398 :
            stream[taskname] = PythonOperator ( task_id=taskname,  ** ( t.kwargs ( [ 'python_callable' ] ) ) ,  default_args=task_opargs , doc_md=doc_md , **callback , **dataset , **other_args , params=dagparams )

    ''' Establish dependencies for each task '''
    for idx, task in enumerate(tasks):
        t = Task(task)
        for depend in t.depend() :
            parent = lookup[depend]
            stream[t.name()].set_upstream(stream[parent])