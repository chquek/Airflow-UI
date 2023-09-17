
'''
For more information, read https://docs.apryse.com/
'''
from system.Apryse import PDFreader , PDFwriter

def readpages ( *args , **context ) :
	task = context['dtp']
	form = task.form
	reader = PDFreader( form['filename'] )
	return reader.getmem()

def merge ( *args , **context ) :
	task = context['dtp']
	form = task.form
	writer = PDFwriter()
	numpages = writer.setmem ( task.stream['Reader1'] )
	writer.use ( [ 2 ])
	numpages = writer.setmem ( task.stream['Reader2'] )
	writer.use ( [ 2 ])
	writer.save ( form['filename'])
	print ("Saved to ", form['filename'] )
	return None
    
