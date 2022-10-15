#google colab
#CMD 1
# Install Java, Spark, Findspark and download a Postgresql driver
!apt-get install openjdk-8-jdk-headless -qq > /dev/null
!wget -q http://www-us.apache.org/dist/spark/spark-2.4.5/spark-2.4.5-bin-hadoop2.7.tgz
!tar xf spark-2.4.5-bin-hadoop2.7.tgz
!pip install -q findspark

!wget https://jdbc.postgresql.org/download/postgresql-42.2.9.jar

# Set Environment Variables
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/content/spark-2.4.5-bin-hadoop2.7"

# Start a SparkSession
import findspark
findspark.init()

#CMD 2
# Create a spark session, configured with Posetgres driver
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('AmazonETL')\
        .config("spark.driver.extraClassPath", "/content/postgresql-42.2.9.jar")\
        .getOrCreate()
        
#CMD 3
# Read in data from S3 Bukets
from pyspark import SparkFiles

url= "https://s3.amazonaws.com/amazon-reviews-pds/tsv/amazon_reviews_us_Beauty_v1_00.tsv.gz"
spark.sparkContext.addFile(url)
df = spark.read.csv(SparkFiles.get('amazon_reviews_us_Beauty_v1_00.tsv.gz'), sep='\t', header=True, inferSchema = True)
df.show(n=20,truncate=False)


#CMD 4
# original dataset information
print((df.count(), len(df.columns)))
# check data types
df.printSchema

#CMD 5
# change 2 columns data types (star_rating, review_date)
from pyspark.sql.types import IntegerType, DateType
from pyspark.sql import Column
casted_df = df.withColumn('star_rating',df['star_rating'].cast(IntegerType()))
casted_df = df.withColumn('review_date',df['review_date'].cast(DateType()))
casted_df.printSchema

#CMD 6
# drop any rows with null or NaN
droped_df = casted_df.dropna()
droped_df.count()
# drop duplicate rows
droped_df.dropDuplicates().count()

#CMD 7
# Seperate into 4 dataframes
review_id_df = droped_df.select('review_id', 'customer_id', 'product_id', 'product_parent','review_date')

clean_products_df = droped_df.select('product_id','product_title')

clean_customers_df = droped_df.select('customer_id')

vine_df = droped_df.select('review_id', 'star_rating','helpful_votes','total_votes','vine')

#CMD 8
# manipulate products_df uniquely to match RDS structure
products_df = clean_products_df.select('product_id','product_title').distinct()
products_df.count()

#CMD 9
# manipulate products_df uniquely to match RDS structure
products_df = clean_products_df.select('product_id','product_title').distinct()
products_df.count()

#CMD 10
# manipulate customer dataframer to get unique customer id and count

customers_df = clean_customers_df.groupBy('customer_id').count()
customers_df = customers_df.withColumnRenamed('count','customer_count')

# descending roder 
customers_df.orderBy(customers_df['customer_count'].desc()).show()

#CMD 11
# manipulate customer dataframer to get unique customer id and count

customers_df = clean_customers_df.groupBy('customer_id').count()
customers_df = customers_df.withColumnRenamed('count','customer_count')

# descending roder 
customers_df.orderBy(customers_df['customer_count'].desc()).show()

#CMD 12
# double confirm star_rating data type is int
vine_df =vine_df.withColumn('star_rating',df['star_rating'].cast(IntegerType()))
vine_df.printSchema

#CMD 13
# upload RDS password file
from google.colab import files
files.upload()

#CMD 14
# set up config parameter
from RDS_config import password

mode = "append"
jdbc_url="jdbc:postgresql://dataviz.caktah2xv07p.us-east-2.rds.amazonaws.com:5432/Amazon_reviews"
config = {"user":"postgres",
          "password": password,  
          "driver":"org.postgresql.Driver"}
          
#CMD 15
# write DataFrame into RDS directly via JDBC

customers_df.write.jdbc(url=jdbc_url,
                         table ='customers', 
                         mode=mode, properties = config)
                         
#CMD 16
products_df.write.jdbc(url=jdbc_url,
                         table ='products', 
                         mode=mode, properties = config)

#CMD 17
review_id_df.write.jdbc(url=jdbc_url,
                         table ='review_id_table', 
                         mode=mode, properties = config)

#CMD 18
vine_df.write.jdbc(url=jdbc_url,
                         table ='vine_table', 
                         mode=mode, properties = config)
