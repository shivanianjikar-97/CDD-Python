from CDD3.iface.iWriter import iWriter
from CDD3.driver.driver import config

class csvWriter(iWriter):

    def write(self, df):
        df.select(config.get("DEFAULT", "targetColumns")) \
                .write() \
                .csv(config.get("DEFAULT", "targetOutputPath"))