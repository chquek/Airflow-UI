'''
https://airflow.apache.org/docs/apache-airflow/1.10.4/_modules/airflow/operators/python_operator.html#PythonOperator.execute
indentation = 2 spaces
'''
from airflow.operators.python import PythonOperator

import os
import pprint
    
class PythonOperator ( PythonOperator ):

  ''' This is the place to amend kwargs before calling the super method. '''
  def __init__(self , *args, **kwargs ):
    ''' set the provide text to True for all task '''
    kwargs['provide_context'] = True

    ''' dtp = dag task pointer.  this is object class Task import Task.  dtp.detail - give list of key/value pairs. KEEP THIS LINE  '''
    self.dtp = kwargs['default_args']['dtp']

    ''' the python callable is a string.  need to resolve it to a module method '''
    callable = self.dtp.detail.python_callable
    kwargs['python_callable'] = self.dtp.module ( callable )

    super().__init__(  *args , **kwargs )

  ''' This is the place to amend code before calling the execute method.  '''
  def execute ( self , context ):

    ''' automatically pull in data from previous stages , KEEP THIS LINE '''
    self.dtp.set_task_instance ( context['task_instance'] )
    self.dtp.xcom_pull ()

    ''' set the dag-task-pointer in context to make it accessible to module  , KEEP THIS LINE '''
    context['dtp'] = self.dtp
    return super().execute( context )
