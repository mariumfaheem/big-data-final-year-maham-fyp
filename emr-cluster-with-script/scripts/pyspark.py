import pyspark
from pyspark.sql.functions import rand
import datetime

from pyspark import SparkContext, HiveContext
from pyspark.sql import SparkSession
import time


# spark_context = SparkContext()
spark = SparkSession.builder.getOrCreate()
# sqlContext = HiveContext(spark_context)

def timer():
    now = time.localtime(time.time())
    return now[5]

def genfacdetail(n):
    start_date = datetime.date(2021, 10, 21)
    end_date   = datetime.date(2022, 10, 20)
    columns = ['Name', 'Label','GlobalID','Country','Company','Industry','Date']


    names = ["DogFood", "HairDryer", "Soap", "Diper", "Apple", "Tape"]*n
    strategy = ["Dave", "John", "Best", "Trial", "Neutral", "HighVol"]*n
    globalid = [ hash('string'+str(x)) for x in range(1,100)]*n
    model = ['Country'+str(x)+'S' for x in range(1,200)]*n
    factors = [ 'Company'+str(x) for x in range(1,2000)]*n
    factorclasses = [ 'Industry$'+str(x) for x in range(1,200)]*n
    dates = [(start_date + datetime.timedelta(n)).strftime('%Y-%m-%d') for n in range(int ((end_date - start_date).days))]*n


    dataframe = spark.createDataFrame(zip(names,strategy,model,factors,factorclasses,globalid,dates), columns)

    dataframe=dataframe.withColumn('Profit', rand(seed=20) * 1000000) \
        .withColumn('Sale', rand(seed=40) * 10000000)

    return dataframe.orderBy(rand())



df = genfacdetail(1000)
df.createOrReplaceTempView('table1')

startTimeQuery = time.clock()
query1="""
SELECT avg(Profit), sum(Sale), max(Sale) 
FROM table1
WHERE Date BETWEEN "2021-11-16" AND "2021-12-13"
GROUP BY Name, Date
"""

profit_with_date_df = spark.sql(query1)
count = profit_with_date_df.count()
print(count)
endTimeQuery = time.clock()
runTimeQuery1 = endTimeQuery - startTimeQuery
runTimeQuery1


startTimeQuery = time.clock()
query_with_label = """
SELECT sum(Profit), sum(Sale), avg(Profit)
FROM table1
WHERE Label="HighVol" or (Country = "Country1S" OR Country = "Country2S" OR Country = "Country3S")
GROUP BY Label, Industry """
query_with_label_df = spark.sql(query_with_label)
count = query_with_label_df.count()
print(count)
endTimeQuery = time.clock()
runTimeQuery2 = endTimeQuery - startTimeQuery
runTimeQuery2


#To Measure how many rows it process in 30sec

while 1 == 1:
    current_sec = timer()
    if current_sec == 30:
        break
    profit_with_date_df.count()

