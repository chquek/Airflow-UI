
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator

class ProduceToTopicOperator ( ProduceToTopicOperator ):

  ''' This is the place to amend kwargs before calling the super method. '''
  def __init__(self , *args, **kwargs ):

    ''' dtp = dag task pointer.  this is object class Task import Task.  dtp.detail - give list of key/value pairs. KEEP THIS LINE  '''
    self.dtp = kwargs['default_args']['dtp']

    ''' the python callable is a string.  need to resolve it to a module method '''
    callable = self.dtp.detail.producer_function
    kwargs['producer_function'] = self.dtp.module ( callable )
    print ( "producer_function" , kwargs['producer_function'] )

    super().__init__(  *args , **kwargs )


# https://airflow.apache.org/docs/apache-airflow-providers-apache-kafka/stable/_modules/airflow/providers/apache/kafka/operators/consume.html#ConsumeFromTopicOperator.execute
class ConsumeFromTopicOperator ( ConsumeFromTopicOperator ):

  ''' This is the place to amend kwargs before calling the super method. '''
  def __init__(self , *args, **kwargs ):
      
    ''' dtp = dag task pointer.  this is object class Task import Task.  dtp.detail - give list of key/value pairs. KEEP THIS LINE  '''
    self.dtp = kwargs['default_args']['dtp']

    ''' the python callable is a string.  need to resolve it to a module method '''
    callable = self.dtp.detail.apply_function
    kwargs['apply_function'] = self.dtp.module ( callable )

    self.results = []
    kwargs['topics'] = self.dtp.detail.topics.split(",")
    kwargs["apply_function_kwargs"] = {"prefix": "consumed:::" , "dataptr" : self.results } 
    kwargs['max_messages'] = 10
    kwargs['max_batch_size'] = 5

    super().__init__(  *args , **kwargs )

  def execute ( self , context ) :

    print ( "kwargs1" , self.apply_function_kwargs ) 
      
    ''' automatically pull in data from previous stages , KEEP THIS LINE '''
    self.dtp.set_task_instance ( context['task_instance'] )
    self.dtp.xcom_pull ()

    ''' set the dag-task-pointer in context to make it accessible to module  , KEEP THIS LINE '''
    context['dtp'] = self.dtp

    # the super.execute returns nothing
    super().execute ( context )
    print ( "kwargs2" , self.apply_function_kwargs ) 
    print ("Results", self.apply_function_kwargs['dataptr'] )
    return self.apply_function_kwargs['dataptr']