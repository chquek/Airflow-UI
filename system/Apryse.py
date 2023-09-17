
'''
For more information, read https://docs.apryse.com/
'''
import os

KEY = "demo:1688081145858:7d92604403000000004098081f3b8141d4d261a141555a584492ac20c5"

# from apryse_sdk import *
from apryse_sdk import PDFNet , PDFDoc , SDFDoc , MappedFile , FilterReader 

class PDFwriter() :

	def __init__ ( self  ) :
		PDFNet.Initialize(KEY)
		self.new_doc=PDFDoc()

	def setmem ( self , bytes ) :
		self.in_doc = PDFDoc(bytearray(bytes), len(bytes) )
		num_pages = self.in_doc.GetPageCount()
		return num_pages

	# use the following pages from the data stream
	def use ( self , pages) :
		for pg in pages :
			self.new_doc.InsertPages( self.new_doc.GetPageCount() + 1  , self.in_doc, pg , pg , PDFDoc.e_none)

	def save ( self , filename ) :
		self.new_doc.Save(filename, SDFDoc.e_remove_unused)
		self.new_doc.Close()

class PDFreader() :

	# read the file into memory
	def __init__ ( self , filename ) :
		PDFNet.Initialize(KEY)
		file = MappedFile(filename)
		file_sz = file.FileSize()
		file_reader = FilterReader(file)
		self.mem = file_reader.Read(file_sz)

	def getmem(self) :
		return self.mem