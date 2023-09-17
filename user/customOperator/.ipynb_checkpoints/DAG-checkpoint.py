
from airflow.operators.python import PythonOperator

from airflow import DAG
import logging

logger = logging.getLogger("kinisi")

class DAG ( DAG ) :

  def __init__(self , *args, **kwargs ):
    '''
    Add in any addition keyword arguments to DAG if it is missing in the designerUI.
    logger.info ( f"CUSTOMDAG - ARGS = {args} , KWARGS = {kwargs}" )
    logger.info ( dir(self) )
    '''
    super().__init__(  *args , **kwargs )
    