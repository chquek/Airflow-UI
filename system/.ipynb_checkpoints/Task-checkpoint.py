import pprint
import json
import os
import pickle
import importlib
import sys

# print statements appear in worker log
class Task :

    # e.g. self.deployment = deployment  , type must correspond to the YAML filename
    def __init__(self, *args, **kwargs):

        self.task = args[0]
        self.STAGING = "/tmp"

        # self.log ( json.dumps(self.task) )

        self.jbody = self.task['body'] 
        self.__setattr__( "body" , self.jbody )

        self.detail = self.jbody['detail']

    # set up the lookup table that gives taskid vs taskname, of all task in this DAG
    def lookup ( self , ptr ):
        self.lookup = ptr 

    # Return the detail part of this class
    def detail(self):
        return self.jbody['detail']

    # ID of this task , which correspond to the id in the task table
    def id(self):
        return self.task['id']

    def log(self,s) :
        f = open("/tmp/log.out","w") 
        f.write ( "Logger" ) 
        f.write ( s ) 
        f.close()

    # get the callback dictionary
    def callback(self,dagname) :

        print ("TASK CLASS CALLBACK" , dagname )
        try :
            callback = self.detail['callback'] 
            print ("CALLBACK in TASK" , callback )
            ans = {}
            for cb in callback :
                funcstr = callback[cb]
                path = funcstr.split('.')
                __routine = path.pop()
                __module = ".".join(path)
                try :
                    mod = __import__( __module )
                    func = getattr( mod, __routine )
                    ans[cb] = func
                except Exception as e :
                    print ( f"callback for {self.name()} - discarding - {e}")

            print ( f"callback for {self.name()} - returning {ans}")
            return ans
        # if callback is missing return {}
        except Exception as e :
            print ("CALLBACK EXCEPTION" ,e )
            pass

        return {}

    # name of task
    def name(self):
        return (self.task['name'])

    def get(self,name) :
        try :
            print ("Get",name,self.detail[name] ) 
            return (self.detail[name] )
        except :
            return None

    # Some task has no dependencies
    def depend (self):
        try :
            # print ( "Depend in task class",self.body['depend'])
            return self.body['depend']
        except :
            return []

    def pickle_push ( self , obj , path  ) :
        id = self.task['id']
        dumpfile = f"{path}/data_{id}" 
        print ( f"Save to {dumpfile}" )
        with open( dumpfile , "wb") as f :
            f.write( pickle.dumps(obj) )

    # Pull data from the task it is dependent on
    def pickle_pull (self , path ):
        result = []
        for ancestor in self.depend() :       
            print ("ancestor",ancestor )
            dumpfile = f"{path}/data_{ancestor}" 
            try :
                with open( dumpfile ,"rb") as f :
                    str = f.read()
                result.append ( pickle.loads ( str ) ) 
            except :  # previous stage did not push in anything
                pass
        return result

    # There is not xcom_push, because that is automatic
    def xcom_pull_TBD(self) :
        result = []
        for parentid in self.depend() :       
            parentname = self.lookup[parentid]
            result.append(parentname)

        print ("xcom_pull from " , result )
        return self.ti.xcom_pull(task_ids=result) 

    # There is not xcom_push, because that is automatic
    def xcom_pull(self) :
        result = {}
        for idx , parentid in enumerate( self.depend() ) :       
            parent= self.lookup[parentid]
            result[parent] = self.ti.xcom_pull(task_ids=parent)
            result[idx] = result[parent]
        return result 


    '''
    How does it know which function to run ?  The job is defined by the DAG in the following line :
    stream[idx] = customPythonOperator ( task_id=f"{taskname}", python_callable=t.run , op_kwargs = {'job' : taskdetail } )
    Print here goes to Airflow logs on the GUI page.  
    '''
    def run ( self , params , **context ):
    
        self.ti = context['task_instance']
        self.params = params
        self.context = context
        self.form = self.detail

        try :
          code = self.get('code')
          arr = code.split('.')
          func = arr.pop()
          module = ".".join(arr)
        except Exception as e :
          print (f"Failed to find Code section in task")
          print (e)
          return

        # in case get("op_kwargs") returns None
        try :
            self.op_kwargs = json.loads(self.get("op_kwargs"))
        except :
            self.op_kwargs = {}

        self.stream = self.xcom_pull ( )
        data = dict(task=self)
        trun = importlib.import_module ( module  )
        ans = eval ( f"trun.{func}(**data)" )
        return ans


if __name__ == '__main__':
	T = Task(task)
	T.push()
	T.pull()