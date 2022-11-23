from CDD3.iface.iWriter import iWriter
from CDD3.impl.tableWriter import bqWriter, oracleWriter

class writerFactory:
    def getFileWriter(self,config) -> iWriter:

        if (config.get("DEFAULT", "target").lower() == "bq"):
            return bqWriter
        elif (config.get("DEFAULT", "target").lower() == "oracle"):
            return oracleWriter
        else:
            print("Invalid target")
