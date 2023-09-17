
'''
https://airflow.apache.org/docs/apache-airflow/stable/howto/operator/datetime.html
'''
from airflow.operators.datetime import BranchDateTimeOperator

import os
import pendulum
from pprint import pprint
import logging

# messages go into logs/airflow.log
logger = logging.getLogger("kinisi")

class BranchDateTimeOperator ( BranchDateTimeOperator ):

  def __init__(self , *args, **kwargs ):

    '''
    The execute method expects the target_lower/upper to be a datetime or time object.  The UI pass this as a string.
    Convert the string into a datetime object.
    '''
    target_lower = kwargs['target_lower']
    target_upper = kwargs['target_upper']

    kwargs['target_lower'] = pendulum.from_format( target_lower , 'YYYY-MM-DD HH:mm')
    kwargs['target_upper'] = pendulum.from_format( target_upper , 'YYYY-MM-DD HH:mm')

    '''
    The execute method expects the follow_ids to be an array of tasks.  The UI pass this as a comma separate value.  
    Convert it to array and strip white spaces.
    '''
    follow_task_ids_if_true = kwargs['follow_task_ids_if_true'].split(",")
    stripped = [ s.strip() for s in follow_task_ids_if_true ]
    kwargs['follow_task_ids_if_true' ] = stripped
      
    follow_task_ids_if_false = kwargs['follow_task_ids_if_false'].split(",")
    stripped = [ s.strip() for s in follow_task_ids_if_false ]
    kwargs['follow_task_ids_if_false' ] = stripped

    self.dtp = kwargs['default_args']['dtp']
      
    ''' Call the parent init method with the modified kwarg values '''
    super().__init__(  *args, **kwargs )

  def execute ( self , context  ):
      
    pprint ( context ) 
    pprint ( self.dtp  )
    print ( context['task_instance'].xcom_pull() )
    ans = super().execute( context )
    return ans