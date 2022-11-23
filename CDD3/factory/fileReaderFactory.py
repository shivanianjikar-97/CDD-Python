from CDD3.iface.iReader import iReader
from CDD3.impl.fileReader import parquetReader, csvReader , gcscsvReader , gcsParquetReader, S3bucketParquetReader, S3bucketcsvReader


class readerFactory:
    def getFileReader(self,config) ->iReader:

        if config.get("DEFAULT", "source").lower() == "hdfs":
            if config.get("DEFAULT", "sourceFileType").lower() == "csv":
                return csvReader()
            elif config.get("DEFAULT", "sourceFileType").lower() == "parquet":
                return parquetReader()
        elif (config.get("DEFAULT", "source").lower() == "gcs"):
            if config.get("DEFAULT", "sourceFileType").lower() == "csv":
                return gcscsvReader()
            elif config.get("DEFAULT", "sourceFileType").lower() == "parquet":
                return gcsParquetReader()
        elif (config.get("DEFAULT", "source").lower() == "aws"):
            if config.get("DEFAULT", "sourceFileType").lower() == "csv":
                return S3bucketcsvReader()
            elif config.get("DEFAULT", "sourceFileType").lower() == "parquet":
                return S3bucketParquetReader()
        else:
            print("Invalid source")


