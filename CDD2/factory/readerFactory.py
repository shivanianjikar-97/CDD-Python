from CDD2.iface.iReader import iReader
from CDD2.impl import parquetReader, csvReader


class readerFactory:
    def getFileReader(self,config) ->iReader:
        if(config.get("DEFAULT" ,"source").lower( )=="csv" ):
            return csvReader()
        elif(config.get("DEFAULT" ,"source").lower( )=="json"):
            return parquetReader()
        else:
            print("Invalid source")


