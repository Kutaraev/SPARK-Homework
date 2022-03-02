import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc


spark = SparkSession.builder.master('local[1]') \
                    .appName('task_3') \
                    .getOrCreate()


train=spark.read.csv("hdfs://sandbox-hdp.hortonworks.com:8020/user/root/data/train.csv",
                     inferSchema='True', header='True').limit(1000)


print('__________________________________________')
print('TOP-3 hotels where people with children are interested but not booked in the end:')

train.filter((col('srch_children_cnt') > 0) & (col('is_booking') == 0)) \
     .groupBy('hotel_continent', 'hotel_country', 'hotel_market') \
     .count().withColumnRenamed('count','searches') \
     .sort(col('searches').desc()) \
     .show(3)
