# templates/airflow.py #

import sys
import os
sys.path.append ("/home/kinisi")
sys.path.append ("/home/airflow")

from system.Task import Task

# customOperator.DAG derived from airflow.DAG
from udm.customOperator.DAG import DAG

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
from udm.customOperator.SQLExecuteQuery import SQLExecuteQueryOperator

logger = logging.getLogger("kinisi")
DAG_ID = "ETLDEMO"

tasks = [{'id': 68, 'pid': 4, 'name': 'Capitalize', 'operator': 'Python', 'body': {'depend': [67], 'detail': {'baseop': 'from udm.customOperator.Python import PythonOperator', 'callback': {}, 'dataset': None, 'doc_md': None, 'python_callable': 'udm.Transform.Capitalize', 'runnable': "PythonOperator ( task_id=taskname,  ** ( t.kwargs ( [ 'python_callable' ] ) ) ,  default_args=task_opargs , doc_md=doc_md , **callback , **dataset , **other_args , params=dagparams )", 'task_opargs': {}, 'task_params': {}, 'trigger': 'all_success', 'other_args': {'trigger_rule': 'all_success'}}}, 'bfield': None}, {'id': 250, 'pid': 4, 'name': 'Create', 'operator': 'SQLExecuteQuery', 'body': {'depend': [68], 'detail': {'baseop': 'from udm.customOperator.SQLExecuteQuery import SQLExecuteQueryOperator', 'callback': {}, 'conn_id': 'kinisi-postgres', 'dataset': None, 'doc_md': None, 'note': 'https://airflow.apache.org/docs/apache-airflow-providers-common-sql/stable/operators.html', 'passthru': True, 'return_last': None, 'runnable': "SQLExecuteQueryOperator(task_id=taskname, ** ( t.kwargs ( [ 'sql' , 'split_statements' , 'return_last' , 'conn_id'  ] ) ) ,  default_args=task_opargs , doc_md=doc_md , **callback , **dataset , **other_args , params=dagparams )", 'split_statements': 'true', 'sql': 'DROP TABLE IF EXISTS newemployee ;\nCREATE TABLE newemployee  (\n        empno   char(40) ,\n        firstname       char(80),\n        lastname        char(80),\n        hiredate        date ,\n        birthdate       date\n);\n', 'task_opargs': {}, 'task_params': {}, 'trigger': 'all_success', 'other_args': {'trigger_rule': 'all_success'}}}, 'bfield': None}, {'id': 345, 'pid': 4, 'name': 'Employee', 'operator': 'SQL', 'body': {'depend': [], 'detail': {'baseop': 'from udm.customOperator.Python import PythonOperator', 'callback': {}, 'dataset': None, 'doc_md': None, 'python_callable': 'udm.SQL.run', 'runnable': "PythonOperator ( task_id=taskname,  ** ( t.kwargs ( [ 'python_callable' ] ) ) ,  default_args=task_opargs , doc_md=doc_md , **callback , **dataset , **other_args , params=dagparams )", 'task_opargs': {}, 'task_params': {}, 'trigger': 'all_success', 'connid': 'kinisi-db2-nossl', 'format': 'dict', 'operation': 'Select', 'provider': 'db2', 'sql': 'select * from employee', 'other_args': {'trigger_rule': 'all_success'}}}, 'bfield': None}, {'id': 67, 'pid': 4, 'name': 'Filter', 'operator': 'Python', 'body': {'depend': [345], 'detail': {'baseop': 'from udm.customOperator.Python import PythonOperator', 'callback': {}, 'dataset': None, 'doc_md': None, 'python_callable': 'udm.Transform.Filter', 'runnable': "PythonOperator ( task_id=taskname,  ** ( t.kwargs ( [ 'python_callable' ] ) ) ,  default_args=task_opargs , doc_md=doc_md , **callback , **dataset , **other_args , params=dagparams )", 'task_opargs': {}, 'task_params': {}, 'trigger': 'all_success', 'other_args': {'trigger_rule': 'all_success'}}}, 'bfield': None}, {'id': 256, 'pid': 4, 'name': 'GENPDF', 'operator': 'PDF', 'body': {'depend': [68], 'detail': {'baseop': 'from udm.customOperator.Python import PythonOperator', 'callback': {}, 'dataset': None, 'doc_md': None, 'python_callable': 'udm.GenPDF.gen', 'runnable': "PythonOperator ( task_id=taskname,  ** ( t.kwargs ( [ 'python_callable' ] ) ) ,  default_args=task_opargs , doc_md=doc_md , **callback , **dataset , **other_args , params=dagparams )", 'task_opargs': {}, 'task_params': {}, 'trigger': 'all_success', 'filename': '/workarea/employee.pdf', 'other_args': {'trigger_rule': 'all_success'}}}, 'bfield': None}, {'id': 73, 'pid': 4, 'name': 'INFO', 'operator': 'Python', 'body': {'depend': [271], 'detail': {'baseop': 'from udm.customOperator.Python import PythonOperator', 'callback': {}, 'dataset': None, 'doc_md': None, 'python_callable': 'udm.Template.niceprint', 'runnable': "PythonOperator ( task_id=taskname,  ** ( t.kwargs ( [ 'python_callable' ] ) ) ,  default_args=task_opargs , doc_md=doc_md , **callback , **dataset , **other_args , params=dagparams )", 'task_opargs': {}, 'task_params': {}, 'trigger': 'all_success', 'other_args': {'trigger_rule': 'all_success'}}}, 'bfield': None}, {'id': 270, 'pid': 4, 'name': 'NEWEmployee', 'operator': 'SQL', 'body': {'depend': [250], 'detail': {'baseop': 'from udm.customOperator.Python import PythonOperator', 'callback': {}, 'dataset': None, 'doc_md': None, 'python_callable': 'udm.SQL.run', 'runnable': "PythonOperator ( task_id=taskname,  ** ( t.kwargs ( [ 'python_callable' ] ) ) ,  default_args=task_opargs , doc_md=doc_md , **callback , **dataset , **other_args , params=dagparams )", 'task_opargs': {}, 'task_params': {}, 'trigger': 'all_success', 'connid': 'kinisi-postgres', 'format': 'dict', 'operation': 'Insert', 'provider': 'postgres', 'sql': 'insert into newemployee ( empno , firstname , lastname , hiredate , birthdate ) values ( ? , ? , ? , ? , ?  )', 'other_args': {'trigger_rule': 'all_success'}}}, 'bfield': None}, {'id': 271, 'pid': 4, 'name': 'SQLFetch', 'operator': 'SQL', 'body': {'depend': [270], 'detail': {'baseop': 'from udm.customOperator.Python import PythonOperator', 'callback': {}, 'dataset': None, 'doc_md': None, 'python_callable': 'udm.SQL.run', 'runnable': "PythonOperator ( task_id=taskname,  ** ( t.kwargs ( [ 'python_callable' ] ) ) ,  default_args=task_opargs , doc_md=doc_md , **callback , **dataset , **other_args , params=dagparams )", 'task_opargs': {}, 'task_params': {}, 'trigger': 'all_success', 'connid': 'kinisi-postgres', 'format': 'dict', 'operation': 'Select', 'provider': 'postgres', 'sql': 'select * from newemployee', 'other_args': {'trigger_rule': 'all_success'}}}, 'bfield': None}, {'id': 255, 'pid': 4, 'name': 'WriteExcel', 'operator': 'Excel', 'body': {'depend': [68], 'detail': {'baseop': 'from udm.customOperator.Python import PythonOperator', 'callback': {}, 'dataset': None, 'doc_md': None, 'python_callable': 'udm.Excel.writer', 'runnable': "PythonOperator ( task_id=taskname,  ** ( t.kwargs ( [ 'python_callable' ] ) ) ,  default_args=task_opargs , doc_md=doc_md , **callback , **dataset , **other_args , params=dagparams )", 'task_opargs': {}, 'task_params': {}, 'trigger': 'all_success', 'filename': '/workarea/employee.xlsx', 'other_args': {'trigger_rule': 'all_success'}}}, 'bfield': None}]
dagparams = AttrDict({'name': 'John', 'age': 34})

dag_kwargs = AttrDict ( {'owner_links': AttrDict({'airflow': 'https://airflow.apache.org'}), 'concurrency': 8, 'catchup': True, 'tags': ['demo', 'etl', 'transform', 'filter', 'database', 'excel', 'demo', 'triggerdag'], 'doc_md': '# Demonstrates using airflow for ETL\n\n- extract from employee table from DB2\n- use the data to write to excel\n- generate a PDF file\n- insert into table in mysql\n\n'}  )
default_args = AttrDict({'retries': 2, 'retry_delay': 'datetime.timedelta(seconds=5)', 'email': ['airflow@example.com'], 'email_on_failure': False, 'email_on_retry': False, 'owner': 'airflow', 'start_date_old': '01/01/2023T01:01', 'start_date': '2023-09-28T00:00'})
default_args['retry_delay'] = datetime.timedelta(seconds=5)

'''
'''
dag_kwargs.owner_links = AttrDict({'airflow': 'https://airflow.apache.org'})
dag_kwargs.tags = ['demo', 'etl', 'transform', 'filter', 'database', 'excel', 'demo', 'triggerdag']

dag_callbacks = AttrDict({})
cleanup = False

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

        if t.id() == 68 :
            stream[taskname] = PythonOperator ( task_id=taskname,  ** ( t.kwargs ( [ 'python_callable' ] ) ) ,  default_args=task_opargs , doc_md=doc_md , **callback , **dataset , **other_args , params=dagparams )
        if t.id() == 250 :
            stream[taskname] = SQLExecuteQueryOperator(task_id=taskname, ** ( t.kwargs ( [ 'sql' , 'split_statements' , 'return_last' , 'conn_id'  ] ) ) ,  default_args=task_opargs , doc_md=doc_md , **callback , **dataset , **other_args , params=dagparams )
        if t.id() == 345 :
            stream[taskname] = PythonOperator ( task_id=taskname,  ** ( t.kwargs ( [ 'python_callable' ] ) ) ,  default_args=task_opargs , doc_md=doc_md , **callback , **dataset , **other_args , params=dagparams )
        if t.id() == 67 :
            stream[taskname] = PythonOperator ( task_id=taskname,  ** ( t.kwargs ( [ 'python_callable' ] ) ) ,  default_args=task_opargs , doc_md=doc_md , **callback , **dataset , **other_args , params=dagparams )
        if t.id() == 256 :
            stream[taskname] = PythonOperator ( task_id=taskname,  ** ( t.kwargs ( [ 'python_callable' ] ) ) ,  default_args=task_opargs , doc_md=doc_md , **callback , **dataset , **other_args , params=dagparams )
        if t.id() == 73 :
            stream[taskname] = PythonOperator ( task_id=taskname,  ** ( t.kwargs ( [ 'python_callable' ] ) ) ,  default_args=task_opargs , doc_md=doc_md , **callback , **dataset , **other_args , params=dagparams )
        if t.id() == 270 :
            stream[taskname] = PythonOperator ( task_id=taskname,  ** ( t.kwargs ( [ 'python_callable' ] ) ) ,  default_args=task_opargs , doc_md=doc_md , **callback , **dataset , **other_args , params=dagparams )
        if t.id() == 271 :
            stream[taskname] = PythonOperator ( task_id=taskname,  ** ( t.kwargs ( [ 'python_callable' ] ) ) ,  default_args=task_opargs , doc_md=doc_md , **callback , **dataset , **other_args , params=dagparams )
        if t.id() == 255 :
            stream[taskname] = PythonOperator ( task_id=taskname,  ** ( t.kwargs ( [ 'python_callable' ] ) ) ,  default_args=task_opargs , doc_md=doc_md , **callback , **dataset , **other_args , params=dagparams )

    ''' Establish dependencies for each task '''
    for idx, task in enumerate(tasks):
        t = Task(task)
        for depend in t.depend() :
            parent = lookup[depend]
            stream[t.name()].set_upstream(stream[parent])