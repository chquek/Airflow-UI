import csv
import json
import ibm_db
import re
from airflow_provider_db2.hooks.db2_hook import DB2Hook

'''
INDENT = spaces
https://github.com/IBM/db2-python/blob/master/Python_Examples/ibm_db/ibm_db-autocommit.py
https://www.ibm.com/docs/en/ias?topic=db-commit-modes
'''

class DB2 ( DB2Hook ):

    def __init__(self, *args , **kwargs ):
        super( DB2 , self ).__init__(  *args, **kwargs)
        self.dbh = self.get_conn()

        self.conn = conn = self.get_conn() 

    
    # Set autocommit to True or False
    def autocommit ( self , flag ) :

        map = {
            True : ibm_db.SQL_AUTOCOMMIT_ON ,
            False : ibm_db.SQL_AUTOCOMMIT_OFF
        }
        try :
            ibm_db.autocommit( self.conn , map[flag] )
            print (f"Set autocommit to - {flag}")
        except Exception as e :
            print (f"Failed to set autocommit - {e}")

    def insert ( self , sqlstmt , predicates=[] , options = {} , raw=False ):
        # if raw is true, dont convert ? to %s
        # for db2 , dont conver ? to %
        pstmt = ibm_db.prepare( self.conn , sqlstmt )
        # Explicitly bind parameters
        for idx, x in enumerate(predicates):
            ibm_db.bind_param(pstmt, idx+1 , x )
        ibm_db.execute(pstmt)

        ans = self.select ("SELECT IDENTITY_VAL_LOCAL() AS VAL FROM SYSIBM.SYSDUMMY1",[],format="dict") 
        print (f"Inserted using conn = {self.conn}" , ans )
        return ans[0]['VAL']

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
        affected = ibm_db.exec_immediate( self.dbh , sqlstmt ) 
        return affected

    def commit(self) :
        ibm_db.commit(self.conn)
        print (f"committed using {self.conn}")

    def rollback(self) :
        ibm_db.rollback(self.conn)
        print (f"rolledback using {self.conn}")

