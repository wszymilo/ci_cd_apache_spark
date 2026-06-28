from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("Spark Example") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .getOrCreate()

inDF = spark.read.format("csv") \
    .option("sep", ",") \
    .option("inferSchema", "true") \
    .option("header", "true") \
    .load("/home/wszymilo/code/PY/ai-eng/ci-cd/data/US_Accidents_March23.csv")

# policz wszystkie wiersze
inDF.count()

# policz wiersze w grupach
inDF.groupBy("State").count().show()

# policz różne wartości
inDF.select(F.countDistinct("City")).show()


from pyspark.sql.window import Window
from pyspark.sql.functions import count, row_number

# Grupujemy dane: ile wypadków dla danej pogody w danym stanie
grouped = inDF.groupBy("State", "Weather_Condition") \
 .agg(count("*") \
 .alias("count"))

windowSpec = Window.partitionBy("State").orderBy(grouped["count"].desc())

# Dodajemy numer wiersza (ranking pogody w stanie)
ranked = grouped.withColumn("rank", row_number().over(windowSpec))

# Filtrujemy tylko najczęstszą pogodę (dla wypadku) w każdym stanie
top_weather_per_state = ranked.filter(ranked["rank"] == 1)
top_weather_per_state.show(50)


# UDF - user defined functions
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def upper_case(text):
   return text.upper() if text else None

# Rejestrujemy funkcję jako UDF
upper_udf = udf(upper_case, StringType())

# Używamy w DataFrame
inDF.withColumn("Weather_Condition_Upper", upper_udf(inDF["Weather_Condition"])).show()


# Spark SQL Api Python
# Dodanie stałej wartości
df2 = inDF.withColumn("stała", F.lit(1))
# Rzutowanie typu (CAST)
#df2 = inDF.withColumn("cena_decimal", F.col("cena").cast("decimal(10,2)"))
# Obliczenia na kolumnach
df2 = inDF.withColumn("czas_minuty",
                   (F.unix_timestamp("End_Time") - F.unix_timestamp("Start_Time")) / 60)
# Tworzenie kolumny warunkowej (when/otherwise)
df2 = inDF.withColumn("pora_dnia",
                   F.when(F.col("Sunrise_Sunset") == "Day", "Dzień")
                    .otherwise("Noc"))
df3 = inDF.withColumn('hour', F.hour(F.col("Start_Time")))


inDF.groupBy("Severity") \
  .avg("Temperature(F)") \
  .orderBy(F.desc("avg(Temperature(F))")).show()

inDF.groupBy("Severity") \
   .count() \
   .orderBy(F.desc("count")).show()


state_stats = inDF.groupBy("State") \
   .count() \
   .filter(F.col("count") > 10000) \
   .orderBy(F.desc("count")).show()




inDF2 = inDF.withColumn("Start_Time", F.to_timestamp("Start_Time")) \
           .withColumn("End_Time", F.to_timestamp("End_Time"))

duration_stats = inDF2.withColumn(
   "Duration_min",
   (F.col("End_Time") - F.col("Start_Time")) / 60
)
