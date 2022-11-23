from pyspark.sql import SparkSession
import configparser
import CDD2.factory.jobFactory as jobFactory

if __name__ == "__main__":
    spark = SparkSession.builder.master("local[1]") \
        .appName('FileRead') \
        .getOrCreate()
    config = configparser.ConfigParser()
    configFilePath = 'cdd2.conf'
    config.read(configFilePath)

    jobFactory().getJob(config,spark)

