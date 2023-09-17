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
from udm.customOperator.SQLExecuteQuery import SQLExecuteQueryOperator

logger = logging.getLogger("kinisi")
DAG_ID = "MS-SQL"

tasks = [{'id': 374, 'pid': 89, 'name': 'AddTable', 'operator': 'SQL', 'body': {'depend': [373], 'detail': {'baseop': 'from udm.customOperator.Python import PythonOperator', 'callback': {}, 'dataset': None, 'doc_md': None, 'python_callable': 'udm.SQL.run', 'runnable': "PythonOperator ( task_id=taskname,  ** ( t.kwargs ( [ 'python_callable' ] ) ) ,  default_args=task_opargs , doc_md=doc_md , **callback , **dataset , **other_args , params=dagparams )", 'task_opargs': {}, 'task_params': {}, 'trigger': 'all_success', 'connid': 'kinisi-mssql', 'format': 'array', 'operation': 'Insert', 'provider': 'mssql', 'sql': 'insert into cars ( make , model , year , price )  values ( ? , ? , ? , ? )', 'other_args': {'trigger_rule': 'all_success'}}}, 'bfield': None}, {'id': 360, 'pid': 89, 'name': 'CarsArray', 'operator': 'SQL', 'body': {'depend': [374], 'detail': {'baseop': 'from udm.customOperator.Python import PythonOperator', 'callback': {}, 'dataset': None, 'doc_md': None, 'python_callable': 'udm.SQL.run', 'runnable': "PythonOperator ( task_id=taskname,  ** ( t.kwargs ( [ 'python_callable' ] ) ) ,  default_args=task_opargs , doc_md=doc_md , **callback , **dataset , **other_args , params=dagparams )", 'task_opargs': {}, 'task_params': {}, 'trigger': 'all_success', 'connid': 'kinisi-mssql', 'format': 'array', 'operation': 'Select', 'provider': 'mssql', 'sql': 'SELECT   * FROM   CARS', 'other_args': {'trigger_rule': 'all_success'}}}, 'bfield': None}, {'id': 358, 'pid': 89, 'name': 'CarsDict', 'operator': 'SQL', 'body': {'depend': [374], 'detail': {'baseop': 'from udm.customOperator.Python import PythonOperator', 'callback': {}, 'dataset': None, 'doc_md': None, 'python_callable': 'udm.SQL.run', 'runnable': "PythonOperator ( task_id=taskname,  ** ( t.kwargs ( [ 'python_callable' ] ) ) ,  default_args=task_opargs , doc_md=doc_md , **callback , **dataset , **other_args , params=dagparams )", 'task_opargs': {}, 'task_params': {}, 'trigger': 'all_success', 'connid': 'kinisi-mssql', 'format': 'dict', 'operation': 'Select', 'provider': 'mssql', 'sql': 'SELECT   * FROM   CARS', 'other_args': {'trigger_rule': 'all_success'}}}, 'bfield': None}, {'id': 365, 'pid': 89, 'name': 'CreateTable', 'operator': 'SQLExecuteQuery', 'body': {'depend': [], 'detail': {'baseop': 'from udm.customOperator.SQLExecuteQuery import SQLExecuteQueryOperator', 'callback': {}, 'conn_id': 'kinisi-mssql', 'dataset': None, 'doc_md': None, 'note': 'https://airflow.apache.org/docs/apache-airflow-providers-common-sql/stable/operators.html', 'passthru': 'False', 'return_last': None, 'runnable': "SQLExecuteQueryOperator(task_id=taskname, ** ( t.kwargs ( [ 'sql' , 'split_statements' , 'return_last' , 'conn_id'  ] ) ) ,  default_args=task_opargs , doc_md=doc_md , **callback , **dataset , **other_args , params=dagparams )", 'split_statements': 'true', 'sql': "\nDROP TABLE IF EXISTS CARS ;\nCREATE TABLE CARS (\n      id INT           NOT NULL    IDENTITY    PRIMARY KEY,\n      make     char(20) ,\n      model    char(20) ,\n      year     int ,\n      price    decimal(7,2)\n);\n\ninsert into CARS ( make , model , year , price ) values ( 'Toyota' , 'Prius' , 2020 , 24000 ) ;\ninsert into CARS ( make , model , year , price ) values ( 'Toyota' , 'Avensis' , 2013, 12000) ;\ninsert into CARS ( make , model , year , price ) values ( 'Toyota' , 'Camry' , 2022, 25000) ;\n\ninsert into CARS ( make , model , year , price ) values ( 'Honda' , 'Accord' , 2022, 25000) ;\n\ninsert into CARS ( make , model , year , price ) values ( 'Nissan' , 'Xtrail' , 2022, 25000) ;", 'task_opargs': {}, 'task_params': {}, 'trigger': 'all_success', 'other_args': {'trigger_rule': 'all_success'}}}, 'bfield': None}, {'id': 373, 'pid': 89, 'name': 'Newcars', 'operator': 'Python', 'body': {'depend': [365], 'detail': {'baseop': 'from udm.customOperator.Python import PythonOperator', 'callback': {}, 'dataset': None, 'doc_md': None, 'python_callable': 'udm.Generator.cars.newmodels', 'runnable': "PythonOperator ( task_id=taskname,  ** ( t.kwargs ( [ 'python_callable' ] ) ) ,  default_args=task_opargs , doc_md=doc_md , **callback , **dataset , **other_args , params=dagparams )", 'task_opargs': {}, 'task_params': {}, 'trigger': 'all_success', 'other_args': {'trigger_rule': 'all_success'}}}, 'bfield': None}, {'id': 359, 'pid': 89, 'name': 'Reader', 'operator': 'Python', 'body': {'depend': [358, 360], 'detail': {'baseop': 'from udm.customOperator.Python import PythonOperator', 'callback': {}, 'dataset': None, 'doc_md': None, 'python_callable': 'udm.Template.niceprint', 'runnable': "PythonOperator ( task_id=taskname,  ** ( t.kwargs ( [ 'python_callable' ] ) ) ,  default_args=task_opargs , doc_md=doc_md , **callback , **dataset , **other_args , params=dagparams )", 'task_opargs': {}, 'task_params': {}, 'trigger': 'all_success', 'other_args': {'trigger_rule': 'all_success'}}}, 'bfield': None}]
dagparams = AttrDict({})

dag_kwargs = AttrDict ( {'owner_links': AttrDict({}), 'concurrency': 8, 'catchup': True, 'tags': ['mssql', 'database'], 'doc_md': '# Header - DAG Markdown notes\n\n- See this for : [default arg options](https://airflow.apache.org/docs/apache-airflow/1.10.8/tutorial.html#default-arguments)\n'}  )
default_args = AttrDict({'owner': 'anybody', 'start_date_old': '2023-08-08T15:14', 'start_date': '2023-09-10T00:00'})

'''
'''
dag_kwargs.owner_links = AttrDict({})
dag_kwargs.tags = ['mssql', 'database']

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

        if t.id() == 374 :
            stream[taskname] = PythonOperator ( task_id=taskname,  ** ( t.kwargs ( [ 'python_callable' ] ) ) ,  default_args=task_opargs , doc_md=doc_md , **callback , **dataset , **other_args , params=dagparams )
        if t.id() == 360 :
            stream[taskname] = PythonOperator ( task_id=taskname,  ** ( t.kwargs ( [ 'python_callable' ] ) ) ,  default_args=task_opargs , doc_md=doc_md , **callback , **dataset , **other_args , params=dagparams )
        if t.id() == 358 :
            stream[taskname] = PythonOperator ( task_id=taskname,  ** ( t.kwargs ( [ 'python_callable' ] ) ) ,  default_args=task_opargs , doc_md=doc_md , **callback , **dataset , **other_args , params=dagparams )
        if t.id() == 365 :
            stream[taskname] = SQLExecuteQueryOperator(task_id=taskname, ** ( t.kwargs ( [ 'sql' , 'split_statements' , 'return_last' , 'conn_id'  ] ) ) ,  default_args=task_opargs , doc_md=doc_md , **callback , **dataset , **other_args , params=dagparams )
        if t.id() == 373 :
            stream[taskname] = PythonOperator ( task_id=taskname,  ** ( t.kwargs ( [ 'python_callable' ] ) ) ,  default_args=task_opargs , doc_md=doc_md , **callback , **dataset , **other_args , params=dagparams )
        if t.id() == 359 :
            stream[taskname] = PythonOperator ( task_id=taskname,  ** ( t.kwargs ( [ 'python_callable' ] ) ) ,  default_args=task_opargs , doc_md=doc_md , **callback , **dataset , **other_args , params=dagparams )

    ''' Establish dependencies for each task '''
    for idx, task in enumerate(tasks):
        t = Task(task)
        for depend in t.depend() :
            parent = lookup[depend]
            stream[t.name()].set_upstream(stream[parent])