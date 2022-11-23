from CDD3.iface.iWriter import iWriter
from CDD3.driver.driver import config

class S3csvWriter(iWriter):

    def write(self, df):
        df.select(config.get("DEFAULT", "targetColumns")).write \
            .option("credentials", "credentials") \
            .option("project", config.get("DEFAULT", "gcpProjectId")) \
            .csv(config.get("DEFAULT", "targetOutputPath"))