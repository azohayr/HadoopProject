from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, to_date, lit
from pyspark.sql import functions as f


'''Create  the session'''
spark = SparkSession.builder.appName("Dataframe Reporting").getOrCreate()

'''load the  files'''
df = spark.read.csv(
    '/Users/admin/OneDrive/Bureau/Projet Hadoop_spark/zip_dataset/output_csv_full.csv',
    header=True, inferSchema=True)
data_country = spark.read.csv(
    '/Users/admin/OneDrive/Bureau/Projet Hadoop_spark/zip_dataset/country_classification.csv',
    header=True, inferSchema=True)
''' Add the colon date, Conversion to format "d/M/YYYY" '''

df = df.withColumn("date", to_date(concat_ws("/", lit(
    "01"), col("time_ref").substr(5, 2), col(
    "time_ref").substr(1, 4)), "d/M/yyyy"))


''' Add the colon Year'''
df = df.withColumn("year", f.year(df["date"]))

''' Associate colon country_name  to country_code   '''

df = df.join(data_country, on="country_code", how="left")

df.show()



