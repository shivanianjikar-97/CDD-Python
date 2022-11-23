from CDD2.iface.iReader import iReader
from CDD2.driver.driver import spark,config

class csvReader(iReader):

    def read(self):
        return spark.read.csv(config.get("DEFAULT","inputFilePath"))