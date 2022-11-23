from CDD3.iface.iReader import iReader
from CDD3.driver.driver import spark,config

class gcscsvReader(iReader):

    def read(self):
        return spark.read.csv(config.get("DEFAULT","inputFilePath"))