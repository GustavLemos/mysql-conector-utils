import mysql.connector

mydb = mysql.connector.connect(
  host="localhost",
  user="yourusername",
  password="yourpassword",
  database="mydatabase"
)

mycursor = mydb.cursor()

#query
mycursor.execute("SELECT * FROM teste.teste")

#array tuples
myresult = mycursor.fetchall()

#plot
for x in myresult:
  print(x)
