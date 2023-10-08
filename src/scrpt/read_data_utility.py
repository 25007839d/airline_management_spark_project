from pyspark.sql import SparkSession
from pyspark.sql.types import*
from pyspark import *

sc = SparkContext.getOrCreate()
spark = SparkSession.builder.getOrCreate()

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



df2 = spark.read.parquet(r"C:\Users\erdus\PycharmProjects\airline data manag\A_data\routes.snappy.parquet")
# df2.show()

#Q-1 """Replace Alias column"""
# a=airlinedf.withColumn('Alias', translate('Alias', "\\", ' ')) # only we can change word to word
# airlinedf.withColumn('Alias',when(airlinedf.Alias.endswith('N'),regexp_replace(airlinedf.Alias,'N','null')) .otherwise(airlinedf.Alias)).show(577) # """It is replaced word by word"""

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
# airlinedf.na.fill({'Alias':'unknown','ICAO':'unknown','IATA':'unknown','Callsign':'unknown','Country':'unknown'})\
#     .replace('\\N','unknown').replace('null','unknown').replace('-','unknown').show()


"""3rd M"""
# airlinedf.select(regexp_replace('ICAO','N/A','unknoww'),regexp_replace('IATA','-','unknoww'),
#                  regexp_replace('IATA','null','unknoww')).show()

"""4th M"""# SQL not perfect
airlinedf.createOrReplaceTempView('airline')
airportdf.createOrReplaceTempView('airport')
routesdf.createOrReplaceTempView('route')
planedf.createOrReplaceTempView('plane')
# spark.sql("select * from airport ").show()
# spark.sql("select * from airline ").show()
# spark.sql("select * from route ").show()
# spark.sql("select * from plane ").show()


# spark.sql(
#     "select Id,name,replace(Alias,'\\N','unknown') alias,nvl(IATA,'unknown')iata,nvl(ICAO,'unknown')icao,"
#     "nvl(Callsign,'unknown')callsign,nvl(country,'unknown')country,active from airline a "
#       ).show()
# spark.sql("select name, CASE WHEN gender = 'M' THEN 'Male' " +
#                "WHEN gender = 'F' THEN 'Female' WHEN gender IS NULL THEN ''" +
#               "ELSE gender END as new_gender from EMP").show()

#Q-3. find the country name which is having both airlines and airport
from pyspark.sql.functions import col

'''1 st M'''
# airlinedf.join(airportdf,on='Country',how='inner').select('Country').filter(airlinedf.Active=='Y').distinct().show()
'''2nd M'''
# airlinedf.select('Country').intersect(airportdf.select('Country')).distinct().show()
'''3rd SQL '''
# spark.sql(
#     "select distinct a.Country from airline a inner join airport b on (a.Country=b.Country ) where a.Active ='Y' "
# ).show()

'''4th SQL interset'''
# spark.sql(
#         "select a.Country from airline a intersect select b.Country from airport b  "
#     ).show()


#Q4- get the airlines details like name, id,. which is has taken takeoff more than 3 times from same airport
'''1st m'''
from pyspark.sql.window import Window
# wind=Window.partitionBy('Name','Country').orderBy('Name')  #--by using single table
# e.withColumn("Country1",row_number().over(wind)).where(col('Country1')>3).show(20000)
'''2nd m'''

aline=airlinedf.withColumnRenamed('Id','airline_id')  # column name chnaged for join
aline.createOrReplaceTempView('aline')
#
# aline.join(routesdf,on='airline_id',how='inner').groupBy('src_airport',aline.airline_id,aline.Name).\
# count().filter(col('count')>3).show()

'''3rd M SQL'''

# spark.sql(
#         " select a.airline_id, a.Name ,r.src_airport,count(*) from aline a join route r on (a.airline_id=r.airline_id)"
#         "group by a.airline_id, a.Name ,r.src_airport having count(*)>3"
#     ).show()

#Q4- get the airlines details like name, id,. which is has taken takeoff less than 2 times from same airport
'''1st by 1 table'''
# wind=Window.partitionBy('Name','Country').orderBy('Name')
# e.withColumn("Country1",row_number().over(wind)).where(col('Country1')<2).select('Id','Name','Country').show(20000)
'''2nd by 2 table'''
#
a = routesdf.select('*').groupBy('src_airport').count().orderBy('count',ascending=True)
# a.show()

b = routesdf.select('*').groupBy('dest_airport').count().orderBy('count',ascending=True)
# b.show()

c= a.union(b).groupBy('src_airport').sum('count').filter(col('sum(count)')==1).select('src_airport')
#
# airportdf.join(c,airportdf.IATA==c.src_airport,'inner').show()

'''3rd M'''
# airportdf.select('*').filter(col('IATA')== c.withColumn('src_airport',c.__getitem__(0)) ).show()
# Q-5 Get the airline details, which is having direct flights. details like airline id, name, source airport name,
a = routesdf.select('*').groupBy('src_airport').count().orderBy('count',ascending=False)
# a.show()
b = routesdf.select('*').groupBy('dest_airport').count().orderBy('count',ascending=False)
# b.show()



# c = a.union(b).groupBy('src_airport').sum('count').orderBy('sum(count)',ascending=False)\
#     .withColumn('rank',rank().over(Window.orderBy(col('sum(count)').desc()))).filter(col('rank')==1).select('src_airport').show()

# airportdf.join(c,airportdf.IATA==c.src_airport,'inner').show()

#=======================================================================================================================
# spark.sql("select * from airport ").show()
# spark.sql("select * from airline ").show()
spark.sql("select * from route ").show()
# spark.sql("select * from plane ").show()

# spark.sql("select dest_airport, max(airline) max ,count(dest_airport) count from route group by dest_airport order by count desc").show()
# spark.sql("select airline, max(dest_airport) max ,count(dest_airport) count from route group by airline order by count desc").show()
# spark.sql("select airline, count(dest_airport) count from route group by airline order by count desc").show()

#Q.7 find the most popular route/s i.e. route with max trips
# required details (src_airport_name,code,airport_id,dest_airport_name ,code, airport_id, total on this route

r=spark.sql("select  max (src_airport) as src_airport from route")
s=spark.sql("select  max (dest_airport) as dest_airport from route")
u=r.union(s)
u.show()
#Q.8 and find which airlines are operational on that route .


