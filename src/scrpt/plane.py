from pyspark.sql import SparkSession
from pyspark.sql.types import*
from pyspark import *

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)
# df1=select
from pyspark.sql.types import DataType
planedf = spark.read.csv(r"C:\Users\erdus\PycharmProjects\airline data manag\A_data\plan.csv")
planedf.show()

# planedf.write.option("header",True).text(r"C:\Users\erdus\PycharmProjects\airline data manag\project\a")
'''
def getListOfReqData(StartString, EndString):
    with open(inputFilePath) as f:
        for line in f:
            # when the StartString is observed
            if line.startswith(StartString):
                myList = []
                # append the 1st line contains the StartString
                myList.append(line.strip())
                for line in f:
                    if line.rstrip() == EndString:
                        # exit when the EndString is encountered
                        break
                    # keep appending all the lines in a List
                    myList.append(line.strip())
    f.close()
    return(myList)

#call the Function
getList = getListOfReqData("data", "#--------------------")

#Convert the List to a Dataframe
DF = spark.createDataFrame(getList,StringType())
DF.cache()

#Apply Split to generate required columns
StructDF = DF.select(split(col("value"), ",").getItem(0).alias("Col1"),
                     split(col("value"), ",").getItem(1).alias("Col2"),
                     split(col("value"), ",").getItem(2).alias("Col3"),
                     split(col("value"), ",").getItem(3).alias("Col4"),
                     split(col("value"), ",").getItem(4).alias("Col5")).drop("value")

StructDF.show(truncate=False)


'''
df.select("name", "favorite_color").write.save("namesAndFavColors.parquet")
dataframe_dropdup = dataframe.dropDuplicates()

# df_fill_by_col = spark.sql("select country,
#                                    name as 'col_name',
#                                    description,
#                                    ct,
#                                    ct_desc,
#                                   (ct*100/ct_desc)
#                             from
#                                ( Select description,
#                                         country,
#                                         count(name) over (PARTITION by description) as ct,
#                                         count(description) over (PARTITION by description) as ct_desc
#                                  from country_name
#                                 )x
#                             group by 1,2,3,4,5,6 ")