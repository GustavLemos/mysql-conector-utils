import pyodbc 
import pandas 

# strings of conection with sql
server = ''
data_base = ''
user = ''
password = ''

cnxn = pyodbc.connect("DRIVER={ODBC Driver 17 for SQL Server};SERVER="+ server +";DATABASE="+ data_base +";UID="+ user +";PWD="+ password)

#string of table
table = ''

cursor = cnxn.cursor()
cursor.execute('SELECT * FROM '+ table)

#print of results of rows
#for row in cursor:
#    print('row = %r' % (row,))

#capture rows whit comand transact sql
rows = cursor.execute('SELECT * FROM '+ table).fetchall() 

#Create dataframe
df = pandas.DataFrame([tuple(t) for t in rows],columns=['id','tipo_jornalista','relevancia','codigo','data_vigencia_inicio','data_vigencia_final']) 
print(df)

#Save dataframe in csv
df.to_csv('file.csv')
