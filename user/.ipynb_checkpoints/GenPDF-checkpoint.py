'''

This page demonstrates getting records from previous stream, and then use it to generate a PDF table of the records


'''

from jinja2 import Environment
import pdfkit
import os
import pprint

myString = '''

<style>
#customers {
  font-family: Arial, Helvetica, sans-serif;
  border-collapse: collapse;
  width: 100%;
}

#customers td, #customers th {
  border: 1px solid #ddd;
  padding: 8px;
}

#customers tr:nth-child(even){background-color: #f2f2f2;}

#customers tr:hover {background-color: #ddd;}

#customers th {
  padding-top: 12px;
  padding-bottom: 12px;
  text-align: left;
  background-color: #04AA6D;
  color: white;
}
</style>

<table id="customers">
    <tr><th>EMPLOYEE NUMBER</th><th>FIRST NAME</th><th>LAST NAME</th><th>HIRE DATE</th><th>BIRTH DATE</th></tr>
    {% for record in records -%}
        <tr>
            {% for item,value in record.items() -%}
                <td>{{ value }}</td>
            {% endfor %}
        </tr>
    {% endfor %}
</table>

'''

'''
From this page :  https://towardsdatascience.com/how-to-easily-create-a-pdf-file-with-python-in-3-steps-a70faaf5bed5
'''
def gen( *args , **context) :

  task = context['dtp']
  Form = task.form

  print ("Form in PDF.py" , Form)
  records = task.stream[0]

  env = Environment()
  tmpl = env.from_string(myString)

  output_text = tmpl.render( records=records  )

  config = pdfkit.configuration(wkhtmltopdf='/usr/bin/wkhtmltopdf')
  fname = Form['filename']
  pdfkit.from_string(output_text, fname , configuration=config)


