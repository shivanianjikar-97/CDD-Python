from CDD3.iface.iReader import iReader
from CDD3.driver.driver import spark,config

class jsonReader(iReader):

    def read(self):
        return spark.read.json(config.get("DEFAULT","inputFilePath"))