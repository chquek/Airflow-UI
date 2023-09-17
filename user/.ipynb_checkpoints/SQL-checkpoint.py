# from .Hooks.MySQL import MySql

from system.sqlhooks.mysql import MySql
from system.sqlhooks.db2 import DB2
from system.sqlhooks.postgres import Postgres
from system.sqlhooks.mssql import MsSQL

import pprint
import re
import logging

logger = logging.getLogger("kinisi")

def info ( *args , **context ) :
    task = context['dtp']
    print ("Form = " , task.form)
    print ("Task Instance" , context['ti'])
    print ("DAG params" , context['params'] )
    print ( f"Info2 , args = {args} , form={task.form}")
    print ("TASK params" , task.form.task_params )
    for i in range(20) :
        try :
            print (f"DATA-{i} - {task.stream[i]}" )
        except :
            pass
    return None

providers = {
    "mssql" : MsSQL ,
    "mysql" : MySql  ,
    "postgres" : Postgres ,
    "db2" : DB2
}

def run ( *args , **context ) :

  task = context['dtp']
  print ("Depend " , task.depend() , "XCOM" , task.xcom_get ("Create") )
  Form = task.form

  # pprint.pprint (Form)
  connid = Form['connid']
  args = {
    "mssql" : { "mssql_conn_id" : connid  } ,
    "mysql" : { "mysql_conn_id" : connid   } ,
    "db2" : { "db2_conn_id" : connid } ,
    "postgres" : { "postgres_conn_id" : connid } ,  
  }
    
  try :
    rows = task.stream[0]
    print ("ROWS",rows)
  except :
    rows = []
      
  # get the database handle using the various hooks provided
  vendor = Form['provider'] 
  hook = providers [ vendor ]
  dbobj = hook ( **args[vendor] )

  
  if ( Form['operation'] == 'Select' ) :
    ans = dbobj.select ( Form['sql'] , format = Form['format'] )
    print ("Select returning" , ans )
    return ans

  if ( Form['operation'] == 'Insert' ) :
    dbobj.autocommit ( False )
    for row in rows :
      if Form['format'] == 'dict' :
        row = list(row.values())
      rowid = dbobj.insert ( Form['sql'] , row , raw=False )  
      print ( f"Inserted : rowid = {rowid} , {row}" )
    dbobj.commit()

  if ( Form['operation'] == 'Delete' ) :
    print (f"Deleting {Form['sql']} " )
    affected = dbobj.delete ( Form['sql'] )  
    print (f"Affected = {affected}") 

  if ( Form['operation'] == 'Update' ) :
    print ("To BE implemented" )
    return None

    