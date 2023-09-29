import csv
import json
from airflow.hooks.mysql_hook import MySqlHook
import MySQLdb.cursors

'''
http://airflow.apache.org/docs/apache-airflow/1.10.8/_api/airflow/hooks/mysql_hook
INDENT = spaces
'''

class MySql ( MySqlHook ):

    def __init__(self, *args , **kwargs ):
        super().__init__(  *args, **kwargs)

        conn = self.get_connection(self.mysql_conn_id)
        print ("after conn" , conn.schema )

        self.dbh = self.get_conn()

        # self.set_autocommit( self.dbh , True ) 
        print("Autocommit = %s" % (self.dbh.get_autocommit() ))

    def select ( self , sqlstmt , predicates = [] , format="array" ) :

        if format == "dict" :
            cursor = self.dbh.cursor( MySQLdb.cursors.DictCursor )
        else :
            cursor = self.dbh.cursor() 
        cursor.execute( sqlstmt, predicates)
        ans = []
        for x in cursor:
            ans.append( x )
        return ans

    # Execute DESCRIBE statement
    def describe ( self , tabname ) : 

        cursor = self.dbh.cursor()
        cursor.execute("DESCRIBE %s" % tabname );

        # Fetch and print the meta-data of the table
        indexList = cursor.fetchall();
        print(indexList);

    def insert ( self , sqlstmt , values=[] , options = {} ):
        cursor = self.dbh.cursor()
        cursor.execute ( sqlstmt , values )

    def delete ( self , sqlstmt , predicates=[] , options = {} ):
        cursor = self.dbh.cursor()
        affected = cursor.execute(sqlstmt, predicates)
        return affected

    def close(self) :
        self.dbh.commit()

