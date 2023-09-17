
''' 
Call by start task to set value of status to 150
'''
def setvalue ( *args , **kwargs ) :
    return dict(status=150)

'''
Call by branch task to return taskname base on value of status
'''
def decision ( *args , **context ) :
    task = context['dtp']
    parentinfo = task.stream[0]
    if parentinfo['status'] > 100 :
        return [ 'MoreThan100' ]
    else :
        return [ 'LessThan100' ]
