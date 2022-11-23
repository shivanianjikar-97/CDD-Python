import configparser
from pyspark.sql import SparkSession

def read_file():

    #add variables for config values
    df = spark.read.text(config.get("DEFAULT","inputFilePath"))

    df = df.selectExpr(config.get("DEFAULT","targetColumns"))

    df.write.format("bigquery") \
            .option("temporaryGcsBucket", config.get("DEFAULT","tempBucketPath")) \
            .option("table", config.get("DEFAULT","targetTableName")) \
            .option("credentials", "credentials") \
            .option("project", config.get("DEFAULT","gcpProjectId")) \
            .save()


if __name__ == "__main__":
    spark = SparkSession.builder.master("local[1]") \
        .appName('FileRead') \
        .getOrCreate()

    config = configparser.ConfigParser()
    configFilePath = 'cdd1.conf'
    config.read(configFilePath)
    read_file()
