from flask import Flask, request
from pyspark.sql import SparkSession

app = Flask(__name__)


@app.route("/result", methods=["POST", "GET"])
def result():
    return fun()


def fun():
    output = {}

    def result_for_query3():
        spark = SparkSession.builder.appName('Read Multiple CSV Files').getOrCreate()

        df = spark.read.csv("/Users/harshitaagrawal/PycharmProject/Assignment/python+spark/Data/*", sep=',',
                            header=True)
        df.groupBy("Stock_Name").count().show()

        df.createOrReplaceTempView("table")

        sqlDf3 = spark.sql("with added_previous_close_table as (select Stock_name,Open,Date,Close,LAG(Close,1,"
                           "35.724998) over(partition by Stock_name order by Date) as previous_close from table) "
                           "select Stock_name, ABS(previous_close-Open) as max_change from added_previous_close_table "
                           "order by max_change DESC ").toPandas()

        pd_output = sqlDf3.to_dict('records')
        output["result"] = pd_output

    result_for_query3()
    return output


if __name__ == '__main__':
    app.run(debug=True, port=2001)