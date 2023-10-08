from pyspark.sql import SparkSession
from pyspark.sql.types import*
from pyspark import *
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

'''Airline'''
airlineSchema = StructType([StructField("Id", dataType=StringType()),
                              StructField("Name", dataType=StringType()),
                              StructField("Alias", StringType()),
                              StructField("IATA", StringType()),
                              StructField("ICAO", StringType()),
                              StructField("Callsign", StringType()),
                              StructField("Country", StringType()),
                              StructField("Active", StringType())
                                ])
airlinedf=spark.read.csv(r"C:\Users\erdus\PycharmProjects\airline data manag\A_data\airline.csv", schema=airlineSchema)
# airlinedf.show(20)
# print(airlineSchema)
from pyspark.sql.functions import translate
from pyspark.sql.functions import when
from pyspark.sql.functions import regexp_replace


'''Airport'''
airportSchema = StructType([StructField("Airport ID", dataType=IntegerType()),
                              StructField("Airport_Name", dataType=StringType()),
                              StructField("City", StringType()),
                              StructField("Country", StringType()),
                              StructField("IATA", StringType()),
                              StructField("ICAO", StringType()),
                              StructField("Latitude", FloatType()),
                              StructField("Longitude", FloatType()),
                                StructField("Altitude", IntegerType()),
                                StructField("Timezone", IntegerType()),
                                StructField("DST", StringType()),
                                StructField("Tz data_base", StringType()),
                                StructField("Type", StringType()),
                                StructField("Zone", StringType())
                                ])
airportdf=spark.read.csv(r"C:\Users\erdus\PycharmProjects\airline data manag\A_data\airport.csv", schema=airportSchema)
# airportdf.show(20)
# print(airportSchema)

'''Route'''
routesdf = spark.read.parquet(r"C:\Users\erdus\PycharmProjects\airline data manag\A_data\routes.snappy.parquet")
# routesdf.show()

'''plane'''
#
plane_Schema = StructType([StructField("Aircraft_name", dataType=StringType()),
                              StructField("IATA", dataType=StringType()),
                              StructField("ICAO", StringType())
                              ])
from pyspark.sql import SparkSession
from pyspark.sql.types import*
from pyspark import *

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)
# df1=select
from pyspark.sql.types import DataType
planedf = spark.read.csv(r"C:\Users\erdus\PycharmProjects\airline data manag\A_data\plan.csv",schema=plane_Schema,sep='',header='True')
# planedf.show()

# planedf.write.text(r"C:\Users\erdus\PycharmProjects\airline data manag\project\a")
# planedf.selectExpr("split(_c0, ' ') as Text_Data_In_Rows_Using_CSV").show(20,False)
# planedf.selectExpr("split(value, ' ')as Text_Data_In_Rows_Using_format_load").show(20,False)
# Function to create a List containing the required lines within specific words


df2 = spark.read.parquet(r"C:\Users\erdus\PycharmProjects\airline data manag\A_data\routes.snappy.parquet")
# df2.show()

#Q-1 """Replace Alias column"""
# a=airlinedf.withColumn('Alias', translate('Alias', "\\", ' ')) # only we can change word to word
airlinedf.withColumn('Alias',when(airlinedf.Alias.endswith('N'),regexp_replace(airlinedf.Alias,'N','null'))\
.otherwise(airlinedf.Alias)).show(577) # """It is replaced word by word"""

from pyspark.sql.functions import when
from pyspark.sql.functions import *
# airlinedf.withColumn("Alias",when(airlinedf.Alias == "/N","dush ").otherwise(airlinedf.Alias)).show() ###1st meth
"""Callsign"""
# c=airlinedf.withColumn("Callsign",translate("Callsign", "\\N","a"))
# c.withColumn("Callsign",when(col("Callsign")== "a", "null").otherwise(c.Callsign)).show()

#Q-2 1. In any of your input file if you are getting \N or null values in your column and that column is of string
# type then put default value as "(unknown)" and if column is of type integer then put -1
"""1st M  """
a=airlinedf.withColumn("Alias",when(col("Alias")=="\\N","unknown")
                       .when(col("Alias")=="null","unknown").otherwise(airlinedf.Alias))

b=a.withColumn("IATA",when(col("IATA")=="-","unknown")
                       .when(a.IATA.isNull(),"unknown").otherwise(a.IATA))

c=b.withColumn("ICAO",when(col("ICAO")=="\\N","unknown")
                       .when(b.ICAO.isNull(),"unknown").otherwise(b.ICAO))

d=c.withColumn("Callsign",when(col("Callsign")=="\\N","unknown")
                       .when(c.Callsign.isNull(),"unknown").otherwise(c.Callsign))
e=d.withColumn("Country",when(col("Country")=="\\N","unknown")
                       .when(c.Country.isNull(),"unknown").otherwise(c.Country))
# e.show()
"""2nd M"""
#Q-3. find the country name which is having both airlines and airport
from pyspark.sql.functions import col
f=e.select("Name","Country").where(col("Country")!="unknown")
q2=f.join(airportdf ,f.Country==airportdf.Country,'left').select(f.Country,f.Name,airportdf.Airport_Name)
# q2.withColumn("Country",col("Country")).groupby("Country").agg(count('Country')).show()#groupby cuntry freq.

#Q4- get the airlines details like name, id,. which is has taken takeoff more than 3 times from same airport
# m3time=e.select('Id','Name','Country').where((e.Country)!='unknown').where((e.Name)!='unknown').show()
from pyspark.sql.window import Window
# wind=Window.partitionBy('Name','Country').orderBy('Name')
# e.withColumn("Country1",row_number().over(wind)).where(col('Country1')>3).show(20000)

#Q4- get the airlines details like name, id,. which is has taken takeoff less than 2 times from same airport
# wind=Window.partitionBy('Name','Country').orderBy('Name')
# e.withColumn("Country1",row_number().over(wind)).where(col('Country1')<2).select('Id','Name','Country').show(20000)

# Q-5 Get the airline details, which is having direct flights. details like airline id, name, source airport name,
# and destination airport name

# routesdf.select('*').where(col('stops')==0).show(12000)
#=======================================================================================================================
#____________________'''SQL Method'''_________________________________________________________________________________

# Q1 In any of your input file if you are getting \N or null values in your column and that column is of string type
# then put default value as "(unknown)" and if column is of type integer then put -1
airlinedf.createOrReplaceTempView('airline')
airportdf.createOrReplaceTempView('airport')
routesdf.createOrReplaceTempView('route')
# spark.sql("select * from airport ").show()
# spark.sql("select * from airline ").show()
spark.sql("select * from route ").show()


#Q-3. find the country name which is having both airlines and airport
# spark.sql('select p.Airport_Name,p.Country,a.Name from airline a, airport p  where p.Country= a.Country')

#Q4- get the airlines details like name, id,. which is has taken takeoff more than 3 times from same airport
# spark.sql('select Airport_Name ,Country from airport group by Airport_Name, Country where Country,').show()


# spark.sql('select Name,Id,Country over(partition by Country)  from airport order by Country ').show()
# spark.sql('select airline,airline_id,src_airport,src_airport_id from ( select  airline_id,src_airport '
#           ', count(airline_id) as max from route group by airline_id,src_airport having count(*)>3)').show()

airlinedf=spark.read.option("header", True).csv(r"C:\Users\erdus\Desktop\Brain Work\cLOUD\PROJECT\RITU\sales.csv")
airlinedf.show(2)