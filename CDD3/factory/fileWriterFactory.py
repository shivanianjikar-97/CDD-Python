from CDD3.iface.iWriter import iWriter
from CDD3.impl.fileWriter import csvWriter, parquetWriter ,gcscsvWriter, gcsParquetWriter, S3csvWriter, S3ParquetWriter


class fileWriterFactory:
    def getFileWriter(self,config) -> iWriter:

        if config.get("DEFAULT", "source").lower() == "hdfs":
            if config.get("DEFAULT", "sourceFileType").lower() == "csv":
                return csvWriter()
            elif config.get("DEFAULT", "sourceFileType").lower() == "parquet":
                return parquetWriter()
        elif (config.get("DEFAULT", "source").lower() == "gcs"):
            if config.get("DEFAULT", "sourceFileType").lower() == "csv":
                return gcscsvWriter()
            elif config.get("DEFAULT", "sourceFileType").lower() == "parquet":
                return gcsParquetWriter()
        elif (config.get("DEFAULT", "source").lower() == "aws"):
            if config.get("DEFAULT", "sourceFileType").lower() == "csv":
                return S3csvWriter()
            elif config.get("DEFAULT", "sourceFileType").lower() == "parquet":
                return S3ParquetWriter()

        else:
            print("Invalid target")
