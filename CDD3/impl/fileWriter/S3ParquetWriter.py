from CDD3.iface.iWriter import iWriter
from CDD3.driver.driver import config

class S3ParquetWriter(iWriter):

    def write(self, df):
        df.write() \
            .option("credentials", "credentials") \
            .option("project", config.get("DEFAULT", "gcpProjectId")) \
            .parquet(config.get("DEFAULT", "targetOutputPath"))