from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Read Multiple CSV Files').getOrCreate()

path = ['/content/authors.csv',
        '/content/book_author.csv']

# creating dataframe
df = spark.read.csv("/Users/harshitaagrawal/PycharmProject/Assignment/python+spark/Data/*", sep=',', header=True)
df.groupBy("Stock_Name").count().show()

df.createOrReplaceTempView("table")

print("Question - 1")
spark.sql("SELECT t1.Date, t1.Stock_name, ((t1.High-t1.Open)*100)/Open as Maximum_positive_pctg from table t1 where (("
          "t1.High-t1.Open)*100)/t1.Open = (SELECT max(((t2.High-t2.Open)*100)/t2.Open) from table t2 where t1.date = "
          "t2.Date)").show()

spark.sql("SELECT t1.Date, t1.Stock_name, ((t1.Open-t1.Low)*100)/Open as Maximum_Negative_pctg from table t1 where (("
          "t1.Open-t1.Low)*100)/t1.Open = (SELECT max(((t2.Open-t2.Low)*100)/t2.Open) from table t2 where t1.date = "
          "t2.Date)").show()

sqlDF1 = spark.sql("SELECT t1.Date, t1.Stock_name, ((t1.High-t1.Open)*100)/Open + ((t1.Open-t1.Low)*100)/Open  as "
                   "Maximum_positive_pctg from table t1 where ((t1.High-t1.Open)*100)/Open + (("
                   "t1.Open-t1.Low)*100)/Open = ( "
                   "SELECT max(((t2.High-t2.Open)*100)/Open + ((t2.Open-t2.Low)*100)/Open) from table t2 where "
                   "t1.date = t2.Date)")
sqlDF1.show()

print("Question - 2")
sqlDf2 = spark.sql("SELECT Date, Stock_name, Volume from table where Volume in (SELECT max(Volume) from table group "
                   "by Date)")
sqlDf2.show()

print("Question - 3")
sqlDF3 = spark.sql("with added_previous_close_table as (select Stock_name,Open,Date,Close,LAG(Close,1,35.724998) over("
                   "partition by Stock_name order by Date) as previous_close from table) select Stock_name,"
                   "ABS(previous_close-Open) as max_change from added_previous_close_table order by max_change DESC ")
sqlDF3.show()

print("Question - 4")
spark.sql("CREATE TEMP VIEW open_price_table AS Select Stock_name, Open from table where Date='2021-07-06T00:00:00'")

spark.sql("CREATE TEMP VIEW high_price_table AS Select Stock_name , Max(High) as High from table group by Stock_name")
spark.sql("CREATE TEMP VIEW result_table AS select t1.Stock_name, t1.High, t2.Open from high_price_table t1 Inner join "
          "open_price_table t2 on t1.Stock_name=t2.Stock_name")

spark.sql("select * from result_table").show()

sqlDF4 = spark.sql("Select t1.Stock_name , t1.High-t1.Open as Maximum_Movement from result_table t1 where "
                   "t1.High-t1.Open = (Select Max(t2.High-t2.Open) from result_table t2)")
sqlDF4.show()

print("Question - 5")
sqlDF5 = spark.sql("SELECT Stock_name, std(Volume) AS Standard_deviation FROM table group by Stock_name")
sqlDF5.show()

print("Question - 6")
sqlDF6 = spark.sql("SELECT Stock_name, avg(Open) as Mean_value from table group by Stock_name ")
sqlDF6.show()

print("Question - 7")
sqlDF7 = spark.sql("SELECT Stock_name, avg(Volume) AS Average_volume from table group by Stock_name")
sqlDF7.show()

print("Question - 8")
# spark.sql( "CREATE TEMP VIEW Average_table AS SELECT Stock_name, avg(Volume) AS Average_volume from table group by
# Stock_name") sqlDF8 = spark.sql("SELECT Stock_name, max(Average_volume) as Highest_average_volume from
# Average_table")
sqlDF8 = spark.sql("SELECT Stock_name, avg(Volume) AS Highest_Average_volume from table group by Stock_name order by "
                   "Highest_Average_volume DESC limit 1 ")
sqlDF8.show()

print("Question - 9")
sqlDF9 = spark.sql("Select Stock_name, max(High) as Highest_price, min(Low) as Lowest_price from table group by "
                   "Stock_name")
sqlDF9.show()
