
def showfailure ( *args , **kwargs ) :
    raise Exception

def produce_output ( *args , **kwargs ) :
    return { 'name' : 'joe' }

def readstream ( *args , **context ) :
    task = context['dtp']
    stage = task.form.task_params.stage
    print ("TO READ TASK params" , stage )
    print ( "ANS" , task.xcom_get ( stage ) ) 
     
    # ans = task.stream ( "
