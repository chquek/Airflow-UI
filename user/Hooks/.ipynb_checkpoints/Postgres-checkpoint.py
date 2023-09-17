import csv
import json
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

        conn = self.get_connection(self.postgres_conn_id)
        print ("after conn" , conn )

        self.dbh = self.get_conn()

    def insert ( self , sqlstmt , values=[] , options = {} ):
        cursor = self.dbh.cursor()
        cursor.execute ( sqlstmt , values )

    def select ( self , sqlstmt , predicates = [] , format="array" ) :

        ans = []
        
        if format == "dict" :
            cursor = self.dbh.cursor(cursor_factory = psycopg2.extras.RealDictCursor)
            cursor.execute( sqlstmt , predicates )
            for row in cursor :
                res = {}
                for key in row :
                    res[key] = row[key]
                ans.append(res)
        else :
            cursor = self.dbh.cursor(  ) 
            cursor.execute( sqlstmt , predicates )
            for x in cursor:
                ans.append(x)

        return ans

    def delete ( self , sqlstmt , predicates=[] , options = {} ):
        cursor = self.dbh.cursor()
        affected = cursor.execute(sqlstmt, predicates)
        return affected

    def close(self) :
        self.dbh.commit()
