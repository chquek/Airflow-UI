from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
import re

# http://www.pymssql.org/en/stable/pymssql_examples.html  uses pymssql

# Subclass the database hook
class MsSQL ( MsSqlHook ):

    def __init__(self, *args , **kwargs ):
        super( MsSQL , self ).__init__(  *args, **kwargs)
        self.dbh = self.get_conn()
        # self.dbh.autocommit( kwargs['autocommit'] ) 

        # there is not method to get commit status for mssql
        # print("Autocommit = %s" % (self.dbh.get_autocommit() ))

    def autocommit ( self , flag ):
        self.dbh.autocommit(flag)

    def select ( self , sqlstmt , predicates = [] , format="array" ) :

        options = {}
        if format == "dict" :
            options["as_dict"] = True 
            
        cursor = self.dbh.cursor( **options )
        cursor.execute( sqlstmt, predicates)
        ans = []
        for x in cursor:
            ans.append( x )
        return ans

    # replace ? with %s
    def insert ( self , sqlstmt , values=[] , options = {} , raw = False ):
        # if raw is true, dont convert ? to %s
        if raw == False :
            sqlstmt = re.sub( "(\s*\?\s*)+" ," %s ",sqlstmt)
        cursor = self.dbh.cursor()
        # mssql wants an array of tuples. and the method is executemany instead of execute
        cursor.executemany ( sqlstmt , [ tuple(values) ] )
        return cursor.lastrowid

    def commit ( self ) :
        self.dbh.commit()

    def rollback (self) :
        self.dbh.rollback()