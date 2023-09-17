import csv
import json
import ibm_db
from airflow_provider_db2.hooks.db2_hook import DB2Hook

'''
http://airflow.apache.org/docs/apache-airflow/1.10.8/_api/airflow/hooks/mysql_hook
INDENT = spaces
'''

class DB2 ( DB2Hook ):

    def __init__(self, *args , **kwargs ):
        super( DB2 , self ).__init__(  *args, **kwargs)
        self.dbh = self.get_conn()
        print ("after conn" , self.dbh  )

        # TODO - how to set autocommit on/off
        # self.set_autocommit( self.dbh , True ) 
        # print("Autocommit = %s" % (self.dbh.get_autocommit() ))

    '''
    
    def select ( self , sqlstmt , predicates=[] , options={} , fetch=None ):

        stmt = ibm_db.prepare(self.dbh, sqlstmt )

        # Explicitly bind parameters
        for idx, x in enumerate(predicates):
            ibm_db.bind_param(stmt, idx , x )

        ibm_db.execute(stmt)
        # Process results
        ans = []
        dictionary = fetch(stmt)
        while dictionary != False:
            ans.append ( dictionary )
            dictionary = fetch(stmt) 

        return ans

    
    run a SELECT statement and return the results as an array of list
    
    def select_array ( self , sqlstmt , predicates=[] , options={} ):
        return self.select ( sqlstmt , predicates , options , fetch=ibm_db.fetch_tuple )

    
    run a SELECT statement and return the results as an array of dictionary
    
    def select_dict ( self , sqlstmt , predicates=[] , options={} ):
        return self.select ( sqlstmt , predicates , options , fetch=ibm_db.fetch_assoc )

    def insert ( self , sqlstmt , predicates=[] , options = {} ):
        # if there is a callable function to execute , run it
        if ('callable' in options):
            options['callable']()

        cursor = self.dbh.cursor()
        cursor.execute ( sqlstmt , predicates )
    '''

    def select ( self , sqlstmt , predicates = [] , format="array" ) :

        if format == 'dict' :
            fetch=ibm_db.fetch_assoc
        else :
            fetch=ibm_db.fetch_tuple

        stmt = ibm_db.prepare(self.dbh, sqlstmt )

        # Explicitly bind parameters
        for idx, x in enumerate(predicates):
            ibm_db.bind_param(stmt, idx , x )

        ibm_db.execute(stmt)
        # Process results
        ans = []
        dictionary = fetch(stmt)
        while dictionary != False:
            ans.append ( dictionary )
            dictionary = fetch(stmt) 

        return ans

    def delete ( self , sqlstmt , predicates=[] , options = {} ):
        cursor = self.dbh.cursor()
        affected = cursor.execute(sqlstmt, predicates)
        return affected

    def close(self) :
        ibm_db.close( self.dbh )

