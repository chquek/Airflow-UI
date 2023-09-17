from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

# http://www.pymssql.org/en/stable/pymssql_examples.html  uses pymssql

# Subclass the database hook
class MsSQL ( MsSqlHook ):

    def __init__(self, *args , **kwargs ):
        super( MsSQL , self ).__init__(  *args, **kwargs)
        self.dbh = self.get_conn()

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