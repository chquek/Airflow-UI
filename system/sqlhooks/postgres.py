import csv
import json
import re
import psycopg2
# from airflow.providers.postgres.hooks import postgres
from airflow.providers.postgres.hooks.postgres import PostgresHook

'''
https://airflow.apache.org/docs/apache-airflow-providers-postgres/stable/_api/airflow/providers/postgres/hooks/postgres/index.html
INDENT = spaces
'''

class Postgres ( PostgresHook  ):

    def __init__(self, *args , **kwargs ):
        super( Postgres , self ).__init__(  *args, **kwargs)

        self.conn = self.get_conn()

    def autocommit ( self , flag ) :
        self.conn.set_session( autocommit=flag )
        print(f"Autocommit = {flag}")

    def insert ( self , sqlstmt , values=[] , options = {} , raw=False ):

        # if raw is true, dont convert ? to %s
        if raw == False :
            sqlstmt = re.sub( "(\s*\?\s*)+" ," %s ",sqlstmt)
        cursor = self.conn.cursor()
        cursor.execute ( sqlstmt , values )
        return cursor.lastrowid

    def select ( self , sqlstmt , predicates = [] , format="array" ) :

        ans = []
        
        if format == "dict" :
            cursor = self.conn.cursor(cursor_factory = psycopg2.extras.RealDictCursor)
            cursor.execute( sqlstmt , predicates )
            for row in cursor :
                res = {}
                for key in row :
                    res[key] = row[key]
                ans.append(res)
        else :
            cursor = self.conn.cursor(  ) 
            cursor.execute( sqlstmt , predicates )
            for x in cursor:
                ans.append(x)

        return ans

    def delete ( self , sqlstmt , predicates=[] , options = {} ):
        cursor = self.conn.cursor()
        affected = cursor.execute(sqlstmt, predicates)
        return affected

    def commit(self) :
        self.conn.commit()

    def rollback(self) :
        self.conn.rollback()
