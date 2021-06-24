from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("find table").master("local[*]") \
 .config("hive.metastore.uris","thrift://localhost:9083") \
 .config("spark.sql.warehouse.dir","/user/hive/warehouse") \
     .enableHiveSupport().getOrCreate();
spark.sparkContext.setLogLevel("ERROR")

df1 = spark.sql("select * from emp")
df2 = spark.sql("select * from test")

df1.select("sno","name").show()

df4 = df2.select("sno","lastname").withColumnRenamed("sno","newsno")

df3 = df1.join(df4, df1.sno == df4.newsno, "inner")

df3.select("sno","name","lastname").show()


#spark.sql("show tables ").show()