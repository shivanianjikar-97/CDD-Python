from pyspark.sql import SparkSession

def read_file():

    df = spark.read.options(header='True', inferSchema='True', delimiter=',').csv("gs:\\files\\input\\")

    df = df.selectExpr("id,name,address")

    df.write.format("bigquery") \
            .option("temporaryGcsBucket", "gs:\\files\\input\\") \
            .option("table", "DestTable") \
            .option("credentials", "credentials") \
            .option("project", "projectId") \
            .save()


if __name__ == "__main__":
    spark = SparkSession.builder.master("local[1]") \
        .appName('FileRead') \
        .getOrCreate()
    read_file()
