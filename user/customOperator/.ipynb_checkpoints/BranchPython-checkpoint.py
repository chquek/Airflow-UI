from airflow.operators.python import BranchPythonOperator

import os
import pprint
    
class BranchPythonOperator ( BranchPythonOperator ):

  '''
  This is the place to amend kwargs before calling the super method.  Example 
  - PythonOperator, the python_callable is a string.  Need to resolve it to a module.  See customOperator/PythonOperator
  - BranchDateTimeOperator , the target upper/lower is a string, need to convert it to a datetime object.  See customOperator/BranchDateTimeOperator
  '''
  def __init__(self , *args, **kwargs ):

    kwargs['provide_context'] = True
    
    ''' dtp = dag task pointer.  this is object class Task import Task 
    dtp.detail - give list of key/value pairs
    '''
    self.dtp = kwargs['default_args']['dtp']
    
    ''' the python callable is a string.  need to resolve it to a module method '''
    callable = self.dtp.detail.python_callable
    kwargs['python_callable'] = self.dtp.module ( callable )
    super().__init__(  *args , **kwargs )

  '''
  This is the place to amend code before calling the execute method.  In this case, it auto pulls data from parent stages and set 
  the dag-task-pointer in the context.
  '''
  def execute ( self , context ):

    self.dtp.set_task_instance ( context['task_instance'] )
    print ( f"dtp - {self.dtp}" )
      
    self.dtp.xcom_pull ()

    ''' set the dag-task-pointer in context to make it accessible to module '''
    context['dtp'] = self.dtp
    return super().execute( context )