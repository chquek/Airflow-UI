import pprint
import json
import os
import pickle
import importlib
import sys
from attrdict import AttrDict
from airflow import Dataset

import logging
import logging.handlers as handlers

# set up logging
logger = logging.getLogger("kinisi")
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
rotatehandle = handlers.RotatingFileHandler('/home/airflow/klogs/kinisi.log', maxBytes=20*1024*1024, backupCount=3)
rotatehandle.setFormatter(formatter)
logger.addHandler(rotatehandle)

# print statements appear in worker log
class Task :

    # e.g. self.deployment = deployment  , type must correspond to the YAML filename
    def __init__(self, *args, **kwargs):

        self.task = args[0]
        self.STAGING = "/tmp"
        try :
            self.dagname = kwargs['dagname']
        except :
            self.dagname = "UNKNOWN"

        # self.log ( json.dumps(self.task) )

        self.jbody = self.task['body'] 
        self.__setattr__( "body" , self.jbody )

        self.detail = AttrDict(self.jbody['detail'])
        self.form = self.detail

    # set up the lookup table that gives taskid vs taskname, of all task in this DAG
    def lookup ( self , ptr ):
        self.lookup = ptr 

    # Return the detail part of this class
    def detail(self):
        return self.jbody['detail']

    # ID of this task , which correspond to the id in the task table
    def id(self):
        return self.task['id']

    # get the callback dictionary , and evaluate to function pointer  e.g.  udm.X.y  , has to get module udm.X
    '''
    arr = funcstr.split('.')
    func = arr.pop()
    module = ".".join(arr)
    trun = importlib.import_module ( module )
    eval ( f"trun.{func}(context)" )

    If there is any exception, return {}
    '''
    def callback(self) :
        try :
            callback = self.detail['callback'] 
            ans = {}
            # if callback string is x.y.z.a.b.c , the module is x.y.z.a.b
            for cb in callback :
                funcstr = callback[cb]      # get the callback string
                arr = funcstr.split('.')    
                func = arr.pop()            # get the function
                module = ".".join(arr)      # get the module
                try :
                    mod = importlib.import_module( module )
                    ans[cb] = getattr( mod, func )
                except Exception as e :
                    logger.error ( f"DAG={self.dagname}, Task={self.name()} , Callback={cb} - {e}")
            return ans
        # if callback is missing return {}
        except Exception as e :
            logger.error (f"CALLBACK EXCEPTION - dagname:{self.dagname}, taskname:{self.name()} , callback={callback} , E={e} " )
            return {}

    # get the outlets uri
    def datasets ( self ) :
        uri = self.get('dataset')
        if uri :
            return { 'outlets' : Dataset( uri ) }
        return {}

    # get markdown and return None if does not exist
    def markdown ( self ) :
        try :
            md = self.get('doc_md')
        except :
            md = None
        return md

    # Task default args , like retry, dtp = dag task pointer
    def task_opargs(self) :
        args = dict(dtp=self)
        args.update(self.get('task_opargs'))
        # return  self.get('task_opargs') 
        # logger.info (f"DAG={self.dagname} , Task={self.name()} , task_opargs={args}")
        return args

    # name of task
    def name(self):
        return (self.task['name'])

    def get(self,name) :
        try :
            # print ("Get",name,self.detail[name] ) 
            return (self.detail[name] )
        except :
            return None

    # return the array with values are keyword arguments.  E.g.  [ A , b ]  A : valueA , B : valueB
    # if it is None , dont add it in
    def kwargs ( self , list ):
        retval = {}
        for v in list :
            if self.get(v):
                retval [v] = self.get(v)
        # logger.info (f"DAG={self.dagname} , Task={self.name()} , kwargs={retval}")
        return retval 

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
        # print ( f"Save to {dumpfile}" )
        with open( dumpfile , "wb") as f :
            f.write( pickle.dumps(obj) )

    # Pull data from the task it is dependent on
    def pickle_pull (self , path ):
        result = []
        for ancestor in self.depend() :       
            dumpfile = f"{path}/data_{ancestor}" 
            try :
                with open( dumpfile ,"rb") as f :
                    str = f.read()
                result.append ( pickle.loads ( str ) ) 
            except :  # previous stage did not push in anything
                pass
        return result
    
    def set_task_instance ( self , ti ) :
        self.ti = ti

    # There is not xcom_push, because that is automatic.  depend returns a list of task numbers.
    def xcom_pull(self) :
        result = {}
        for idx , parentid in enumerate( self.depend() ) :       
            parent= self.lookup[parentid]
            result[parent] = self.ti.xcom_pull(task_ids=parent)
            result[idx] = result[parent]
        self.stream = result
        return result

    def xcom_get(self,stage) :
        return self.ti.xcom_pull(task_ids=stage)


    '''
    set the code to run and return the run function instead.  Intercept this so that I can get all the
    task_instance context, pass parameters etc
    '''
    def name_to_module ( self , path ) :
        self.code = path
        return self.run
    
    # given the name x.y.z.a , the module is x.y.z and the method is a.  return the module resolving to x.y.z.a
    def module ( self , path ) :
        arr = path.split('.')
        func = arr.pop()
        module_name = ".".join(arr)
        module_code = importlib.import_module ( module_name )
        return getattr(module_code,func)

if __name__ == '__main__':
	T = Task(task)
	T.push()
	T.pull()
        
def taskrun() :
    print ("Task Run")