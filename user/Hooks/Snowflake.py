import csv
import json
import logging

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

'''
https://airflow.apache.org/docs/apache-airflow-providers-snowflake/stable/_api/airflow/providers/snowflake/hooks/snowflake/index.html#module-airflow.providers.snowflake.hooks.snowflake
INDENT = spaces
'''

logger = logging.getLogger("kinisi")
class Snowflake ( SnowflakeHook ):

    def __init__(self, *args , **kwargs ):
        super().__init__(  *args, **kwargs)

        conn = self.get_connection(self.snowflake_conn_id )
        print ("after conn" , conn.schema )

        self.dbh = self.get_conn()

        # TODO - autocommit
        # self.set_autocommit( self.dbh , True ) 
        # print("Autocommit = %s" % (self.dbh.get_autocommit() ))

    def select ( self , sqlstmt , predicates = [] , format="array" ) :

        cursor = self.dbh.cursor() 
        cursor.execute( sqlstmt, predicates)
        ans = []
        for x in cursor:
            ans.append( x )
        return ans

    def close(self) :
        self.dbh.commit()

