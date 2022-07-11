from flask import Flask, request
from pyspark.sql import SparkSession

app = Flask(__name__)

# There should be all end points in a single flask file(server file)
# queries should be more apporipriate as per the requirement of the question
@app.route("/result", methods=["POST", "GET"])
def result():
    return fun()


def fun():
    output = {}

    def result_for_query1():
        spark = SparkSession.builder.appName('Read Multiple CSV Files').getOrCreate()

        df = spark.read.csv("/Users/harshitaagrawal/PycharmProject/Assignment/python+spark/Data/*", sep=',',
                            header=True)
        df.groupBy("Stock_Name").count().show()

        df.createOrReplaceTempView("table")

        sqlDF1 = spark.sql(
            "SELECT t1.Date, t1.Stock_name, ((t1.High-t1.Open)*100)/Open + ((t1.Open-t1.Low)*100)/Open  as "
            "Total_deviation from table t1 where ((t1.High-t1.Open)*100)/Open + ((t1.Open-t1.Low)*100)/Open = ("
            "SELECT max(((t2.High-t2.Open)*100)/Open + ((t2.Open-t2.Low)*100)/Open) from table t2 where t1.date = "
            "t2.Date)").toPandas()

        pd_output = sqlDF1.to_dict('records')
        output["result"] = pd_output

    result_for_query1()
    return output


if __name__ == '__main__':
    app.run(debug=True, port=2001)
