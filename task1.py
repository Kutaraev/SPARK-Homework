import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc


spark = SparkSession.builder.master('local[1]') \
                    .appName('task_1') \
                    .getOrCreate()


train=spark.read.csv("hdfs://sandbox-hdp.hortonworks.com:8020/user/root/data/train.csv",
                     inferSchema='True', header='True').limit(1000)


print('__________________________________________')
print('TOP-3 most popular hotels between couples:')
train.filter(col('srch_adults_cnt') == 2) \
     .groupBy('hotel_continent', 'hotel_country', 'hotel_market') \
     .count().withColumnRenamed('count','searches') \
     .sort(col('searches').desc()) \
     .show(3)
