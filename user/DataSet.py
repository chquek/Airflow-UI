import pprint

def touch ( *args , **kwargs ) :
    f = open("/workarea/file2.txt" , "w" )
    f.write ("new stuff")
    f.close()