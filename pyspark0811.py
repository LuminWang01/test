from pyspark import SparkContext, SparkConf  
from pyspark.sql import SparkSession  
from pyspark.sql.functions import col, desc, rank, when, lit  
from pyspark.sql.window import Window  
import os

# 初始化Spark会话  
spark = SparkSession.builder.appName("StudentGradeRanking").enableHiveSupport().getOrCreate()  
sc = SparkContext.getOrCreate()


# 读取Hive表（存储在HDFS）
spark.sql('drop table if exists gh0421;')
spark.sql('create table if not exists gh0421(name string,age int) STORED AS parquet ;')
spark.sql("insert into gh0421 values('gh',28);")
spark.sql("insert into gh0421 values('dn',27);")
df = spark.table("gh0421")
df.show()
  
# 连接到MySQL数据库并写入结果  
jdbcDF = (spark.read  
      .format("jdbc")  
      .option("url", "jdbc:mysql://hadoop1:3306/gh04")
      .option("dbtable", "gh0421")
      .option("user", "root")
      .option("password", "1234")
      .load() )
 
jdbcDF.show()
# 停止Spark会话  
spark.stop()
#test modify
