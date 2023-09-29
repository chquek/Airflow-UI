import csv
import json
import logging
import pymongo

from airflow.providers.mongo.hooks.mongo import MongoHook
'''
https://airflow.apache.org/docs/apache-airflow-providers-snowflake/stable/_api/airflow/providers/snowflake/hooks/snowflake/index.html#module-airflow.providers.snowflake.hooks.snowflake
INDENT = spaces
'''

logger = logging.getLogger("kinisi")
class Mongo ( MongoHook ):

    def __init__(self, *args , **kwargs ):

        super().__init__ ( conn_id = "kinisi-mongo" , username = "root" , password = "3388quek" )
    
        self.client = self.get_conn()
        
        print ("CLIENT" , self.client )

        # TODO - autocommit
        # self.set_autocommit( self.dbh , True ) 
        # print("Autocommit = %s" % (self.dbh.get_autocommit() ))

    def select ( self , sqlstmt , predicates = [] , format="array" ) :

        mydb = self.client['tstdb']
        mycol = mydb["movie"]
        print ("MYDB" , mydb , "MYCOL",mycol)
        myquery = {}
        mydoc = mycol.find(myquery)
        print ("MYDOC",mydoc)
        for x in mydoc:
            print ("X" , x )
        return None




    def close(self) :
        self.dbh.commit()

