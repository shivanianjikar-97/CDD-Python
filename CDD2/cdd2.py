import configparser
from pyspark.sql import SparkSession

from abc import ABC, abstractmethod
import cx_Oracle

config = configparser.ConfigParser()
configFilePath = 'cdd2.conf'
config.read(configFilePath)

#create interface for reader and writer and implement in class
#implement in class make common read method and call as required
# for oracle create connection , add details in config


class reader(ABC):
    @abstractmethod
    def read(self) -> bool:
        pass




class jsonReader(reader):

    def read(self, msg):
        return spark.read.json(config.get("DEFAULT","inputFilePath"))


class writer(ABC):
    @abstractmethod
    def write(self) -> bool:
        pass


class bqWriter(reader):

    def write(self, df):

        df.write.format("bigquery") \
            .option("temporaryGcsBucket", config.get("DEFAULT", "tempBucketPath")) \
            .option("table", config.get("DEFAULT", "targetTableName")) \
            .option("credentials", "credentials") \
            .option("project", config.get("DEFAULT", "gcpProjectId")) \
            .save()

class oracleWriter(reader):
    global con
    def createConnection(self):
        try:
            con = cx_Oracle.connect('tiger/scott@localhost:1521/xe')
        except cx_Oracle.DatabaseError as er:
            print('There is an error in Oracle database:', er)
        finally:
            con.close()

    def write(self, df):
        #add connection part and write
        #check without connection
        return spark.read.json(config.get("DEFAULT","inputFilePath"))





if __name__ == "__main__":
    spark = SparkSession.builder.master("local[1]") \
        .appName('FileRead') \
        .getOrCreate()

    csv_df = csvReader()
    csv_df.read("CSV")
    json_df = jsonReader().read("JSON")



