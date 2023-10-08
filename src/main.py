
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("new").getOrCreate()

rdd=spark.read.csv(r"D:\Brainwork\Spark\garage_data\employee_tab5.csv",header=True)
rdd.show()