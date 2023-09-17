
import os
import sys

import pprint

def Filter ( *args , **context ):

	task = context['dtp']
	records = task.stream[0]
	for rec in records :
		for col in [ 'EDLEVEL' , 'BONUS' , 'COMM' , 'PHONENO' , 'SALARY' , 'SEX' , 'JOB' , 'MIDINIT' , 'WORKDEPT' ] :
			del rec[col]
		print ( rec )

	return records

def Capitalize( *args , **context ) :

	task = context['dtp']

	'''
	Capitalize the first name , and job description, and discard the columns BONUS ,COMM , PHONENO and SALARY
	data0 - bc only 1 ancestor
	'''
	records = task.stream[0]
	for rec in records :
		rec['FIRSTNME'] = rec['FIRSTNME'].capitalize()
		rec['LASTNAME'] = rec['LASTNAME'].capitalize()
		rec['HIREDATE'] = rec['HIREDATE'].strftime('%Y/%m/%d')
		rec['BIRTHDATE'] = rec['BIRTHDATE'].strftime('%Y/%m/%d')
		print ( rec )

	return records
