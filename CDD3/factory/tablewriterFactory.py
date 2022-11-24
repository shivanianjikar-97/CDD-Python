from CDD3.iface.iWriter import iWriter
from CDD3.impl.tableWriter import bqWriter, oracleWriter


class tablewriterFactory:
    def gettableWriter(self,config) -> iWriter:

        if config.get("DEFAULT", "targetType").lower() == "table":
            if config.get("DEFAULT", "target").lower() == "bq":
                return bqWriter()
            elif config.get("DEFAULT", "target").lower() == "oracle":
                return oracleWriter()
        else:
            print("Invalid target")
