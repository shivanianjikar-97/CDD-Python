from CDD3.iface.iWriter import iWriter
from CDD3.driver.driver import config

class parquetWriter(iWriter):

    def write(self, df):
        df.write().parquet(config.get("DEFAULT", "targetOutputPath"))