from CDD3.iface.iWriter import iWriter
from CDD3.driver.driver import config

class gcscsvWriter(iWriter):

    def write(self, df):
        df.select(config.get("DEFAULT", "targetColumns")) \
            .write() \
            .option("projectId", config.get("DEFAULT", "projectId")) \
            .option("credentials", config.get("DEFAULT", "credentials")) \
            .csv(config.get("DEFAULT", "targetOutputPath"));