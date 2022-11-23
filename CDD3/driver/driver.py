from pyspark.sql import SparkSession
import configparser
import CDD3.factory.jobFactory as jobFactory

if __name__ == "__main__":
    spark = SparkSession.builder.master("local[1]") \
        .appName('FileRead') \
        .getOrCreate()
    config = configparser.ConfigParser()
    configFilePath = 'cdd2.conf'
    config = config.read(configFilePath)

    jobFactory().getJob(config,spark)

