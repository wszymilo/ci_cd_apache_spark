import kagglehub
from pyspark.sql import SparkSession


path = kagglehub.dataset_download("sobhanmoosavi/us-accidents")
print(path)

spark = SparkSession.builder \
   .appName("Spark Example") \
   .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
   .getOrCreate()

inDF = spark.read.format("csv") \
 .option("sep", ",") \
 .option("inferSchema", "true") \
 .option("header", "true") \
 .load(path + "/US_Accidents_March23.csv")

inDF.printSchema()

inDF.select("city").groupBy("city").count().orderBy("count", ascending=False).show(20)

# Lazy operation
filtered_data = inDF.select("Weather_Condition").groupBy("Weather_Condition").count().orderBy("count", ascending=False)
filtered_data.show(20)


