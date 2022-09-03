'''
Top 10 стран с максимальным зафиксированным кол-вом новых случаев за
последнюю неделю марта 2021 в отсортированном порядке по убыванию
(в выходящем датасете необходимы колонки: число, страна, кол-во новых случаев)
'''
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import to_date
from pyspark.sql.window import Window
from pyspark.sql.functions import max
from pyspark.sql.functions import row_number


spark = SparkSession.builder.appName('spark2').getOrCreate()
df = spark.read.option("header", True).csv("owid-covid-data.csv")
df = df.where('iso_code not like "OWID%"').select("date", "location", "new_cases").withColumn("date", to_date(col("date"), "yyyy-MM-dd")).withColumn("new_cases", df.new_cases.cast('int')).where("date between '2020-03-29' and '2020-03-31'")
windowSpec = Window.partitionBy("location")
df_max = df.withColumn("max_new_cases", max("new_cases").over(windowSpec)).where("max_new_cases = new_cases")
windowSpec = Window.partitionBy("location").orderBy("date")
df_final = df_max.withColumn("row", row_number().over(windowSpec)).where("row = 1").drop("row", "new_cases").orderBy(col("max_new_cases").desc()).limit(10)
df_final.write.mode("overwrite").csv('march_top.csv')