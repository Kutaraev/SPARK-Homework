import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc


spark = SparkSession.builder.master('local[1]') \
                    .appName('task_2') \
                    .getOrCreate()


train=spark.read.csv("hdfs://sandbox-hdp.hortonworks.com:8020/user/root/data/train.csv",
                     inferSchema='True', header='True').limit(50000)

print('Most popular country where hotels are booked and searched from the same country:')
train.filter((col('hotel_country') == col('user_location_country')) & (col('is_booking')== 1)) \
     .groupBy('hotel_country').count().withColumnRenamed('count','searches') \
     .sort(col('searches').desc()) \
     .show(1)
