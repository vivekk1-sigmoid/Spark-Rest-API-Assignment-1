from flask import Flask, request
from pyspark.sql import SparkSession

app = Flask(__name__)


@app.route("/result", methods=["POST", "GET"])
def result():
    return fun()


def fun():
    output = {}

    def result_for_query4():
        spark = SparkSession.builder.appName('Read Multiple CSV Files').getOrCreate()

        df = spark.read.csv("/Users/harshitaagrawal/PycharmProject/Assignment/python+spark/Data/*", sep=',',
                            header=True)
        df.groupBy("Stock_Name").count().show()

        df.createOrReplaceTempView("table")

        spark.sql(
            "CREATE TEMP VIEW open_price_table AS Select Stock_name, Open from table where Date='2021-07-06T00:00:00'")

        spark.sql(
            "CREATE TEMP VIEW high_price_table AS Select Stock_name , Max(High) as High from table group by Stock_name")
        spark.sql(
            "CREATE TEMP VIEW result_table AS select t1.Stock_name, t1.High, t2.Open from high_price_table t1 Inner "
            "join open_price_table t2 on t1.Stock_name=t2.Stock_name")

        spark.sql("select * from result_table").show()

        sqlDF4 = spark.sql("Select t1.Stock_name , t1.High-t1.Open as Maximum_change from result_table t1 where "
                           "t1.High-t1.Open = (Select Max(t2.High-t2.Open) from result_table t2)").toPandas()

        pd_output = sqlDF4.to_dict('records')
        output["result"] = pd_output

    result_for_query4()
    return output


if __name__ == '__main__':
    app.run(debug=True, port=2001)