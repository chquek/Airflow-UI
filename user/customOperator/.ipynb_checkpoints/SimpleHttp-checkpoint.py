import json
from airflow.providers.http.operators.http import SimpleHttpOperator

class SimpleHttpOperator ( SimpleHttpOperator ):

  def __init__(self , *args, **kwargs ):
    ''' set the provide text to True for all task '''
    # kwargs['provide_context'] = True

    ''' dtp = dag task pointer.  this is object class Task import Task.  dtp.detail - give list of key/value pairs. KEEP THIS LINE  '''
    self.dtp = kwargs['default_args']['dtp']

    ''' preprocessing , kwargs[data] is json.  operator expecting a string '''
    kwargs['data'] = json.dumps( kwargs['data'] )

    super().__init__(  *args , **kwargs )

  # the op_kwargs is defined in the DAG itself
  def execute ( self , context ):

    ''' automatically pull in data from previous stages , KEEP THIS LINE '''
    self.dtp.set_task_instance ( context['task_instance'] )
    self.dtp.xcom_pull ()

    ''' set the dag-task-pointer in context to make it accessible to module  , KEEP THIS LINE '''
    context['dtp'] = self.dtp
    return super().execute( context )
