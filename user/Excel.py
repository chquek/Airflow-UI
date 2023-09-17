import pprint
import os
import xlsxwriter
import pandas as pd
import datetime 

'''
https://www.digitalocean.com/community/tutorials/pandas-read_excel-reading-excel-file-in-python
'''

def reader( *args , **context):

    task = context['dtp']
    Form = task.form

    fname = "/workarea/" + Form['filename']
    print (f"File {fname}") 

    # read by default 1st sheet of an excel file
    if Form['Return Type'] == 'Panda' :
        df = pd.read_excel(fname)
        print (f"ME = {__file__} , Returning panda , {Form}")
        return df

def writer( *args , **context ) :

    task = context['dtp']
    Form = task.form
    rows = task.stream[0]

    fname = Form['filename']
    print (f"File {fname}") 

    workbook = xlsxwriter.Workbook(fname)
    worksheet = workbook.add_worksheet()
  
    for rowidx , row in enumerate(rows) :
        for colidx , item in enumerate(row.values()) :
            worksheet.write_string(rowidx,colidx,item)

    workbook.close()
