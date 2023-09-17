import logging

logger = logging.getLogger("kinisi")

def dag_success_callback(context) :
    dagname = (context['dag'].dag_id)
    logger.info( f"DAG:{dagname} success callback" )

def dag_failure_callback(context) :
    dagname = (context['dag'].dag_id)
    logger.error( f"DAG:{dagname} failure callback" )
