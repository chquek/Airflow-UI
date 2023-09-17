
'''
'''
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

import os
import pprint

class SQLExecuteQueryOperator ( SQLExecuteQueryOperator ):

  '''
  Call the parent init method.  Modify the input kwargs here if required
  '''
  def __init__(self , *args, **kwargs ):
    self.dtp = kwargs['default_args']['dtp']
    super().__init__(  *args, **kwargs)

  '''  Execute the SQL.  '''
  def execute ( self , context ):

    ''' set the dag-task-pointer in context to make it accessible to module  , KEEP THIS LINE '''  
    self.dtp.set_task_instance ( context['task_instance'] )
    context['dtp'] = self.dtp
    
    ans = super().execute( context )

    print ( "FORM",self.dtp.form )
    ''' if passthru is set , read data stream-0 and pass it along '''
    if self.dtp.form.passthru is True :
        data = self.dtp.xcom_pull()
        return data[0]
        
    return ans