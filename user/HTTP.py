from airflow.providers.http.hooks.http import HttpHook
import json

'''
https://airflow.apache.org/docs/apache-airflow-providers-http/stable/_api/airflow/providers/http/hooks/http/index.html
'''

def get ( *args , **context ) :
    task = context['dtp']
    form = task.form
    connid = form['connid']
    hook = HttpHook ( http_conn_id = connid )
    hook.method = "GET"
    headers = form['headers']         # convert string to json
    data = form['data']
    resp = hook.run( form['endpoint'] , data , headers , extra_options=None)
    return json.loads(resp.content)

def post ( *args , **context ) :
    task = context['dtp']
    form = task.form
    connid = form['connid']
    hook = HttpHook ( http_conn_id = connid )
    hook.method = "POST"
    headers = form['headers']         # convert string to json
    data = form['data']
    resp = hook.run( form['endpoint'] , json.dumps(data) , headers , extra_options=None)
    return resp.json()

