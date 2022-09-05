'''Выберите 15 стран с наибольшим процентом переболевших на 31 марта
(в выходящем датасете необходимы колонки:
iso_code, страна, процент переболевших)
'''

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName('spark1').getOrCreate()

covid_df = spark.read.option("header", True).csv("owid-covid-data.csv")

covid_df.where('date == "2021-03-31"').where('iso_code not like "OWID%"').withColumn("percent", covid_df.total_cases/covid_df.population*100).select("iso_code", "location", "percent").orderBy(col("percent").desc()).limit(15).write.mode("overwrite").option("header", True).csv('1.csv')
