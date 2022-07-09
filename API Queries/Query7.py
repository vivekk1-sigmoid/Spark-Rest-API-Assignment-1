from flask import Flask, request
from pyspark.sql import SparkSession

app = Flask(__name__)


@app.route("/result", methods=["POST", "GET"])
def result():
    return fun()


def fun():
    output = {}

    def result_for_query7():
        spark = SparkSession.builder.appName('Read Multiple CSV Files').getOrCreate()

        df = spark.read.csv("/Users/harshitaagrawal/PycharmProject/Assignment/python+spark/Data/*", sep=',',
                            header=True)
        df.groupBy("Stock_Name").count().show()

        df.createOrReplaceTempView("table")

        sqlDF7 = spark.sql("SELECT Stock_name, avg(Volume) AS Average_volume from table group by Stock_name").toPandas()

        pd_output = sqlDF7.to_dict('records')
        output["result"] = pd_output

    result_for_query7()
    return output


if __name__ == '__main__':
    app.run(debug=True, port=2001)