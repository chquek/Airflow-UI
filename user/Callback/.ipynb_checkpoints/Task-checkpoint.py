
def task_success_callback(context) :
    dagname = (context['dag'].dag_id)
    task = context['task'].task_id
    print (f"====> {__file__} : {dagname}/{task} success callback")

def task_execute_callback(context) :
    dagname = (context['dag'].dag_id)
    task = context['task'].task_id
    print (f"====> {__file__} : {dagname}/{task} execute callback")

def task_retry_callback(context) :
    dagname = (context['dag'].dag_id)
    task = context['task'].task_id
    print (f"====> {__file__} : {dagname}/{task} retry callback")

def task_failure_callback(context) :
    dagname = (context['dag'].dag_id)
    task = context['task'].task_id
    print (f"====> {__file__} : {dagname}/{task} failure callback")

