'''Посчитайте изменение случаев относительно предыдущего дня в России за
последнюю неделю марта 2021.
(например: в россии вчера было 9150 , сегодня 8763, итог: -387)
(в выходящем датасете необходимы колонки:
число, кол-во новых случаев вчера, кол-во новых случаев сегодня, дельта)
'''

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import to_date
from pyspark.sql.functions import lag
from pyspark.sql.window import Window


spark = SparkSession.builder.appName('spark3').getOrCreate()

df = spark.read.option("header", True).csv("owid-covid-data.csv")
df = df.where('location = "Russia"').select("date", "new_cases").withColumn("date", to_date(col("date"), "yyyy-MM-dd")).withColumn("new_cases", df.new_cases.cast('int'))

ws = Window.partitionBy().orderBy("date")

df = df.withColumn("yesterday", lag("new_cases", 1, 0).over(ws))
df = df.withColumn("delta", df.new_cases - df.yesterday).where("date between '2021-03-29' and '2021-03-31'")
df.write.mode("overwrite").option("header", True).csv("3.csv")