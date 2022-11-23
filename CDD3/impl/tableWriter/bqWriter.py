from CDD3.iface.iWriter import iWriter
from CDD3.driver.driver import config

class bqWriter(iWriter):

    def write(self, df):
        df.write.format("bigquery") \
            .option("temporaryGcsBucket", config.get("DEFAULT", "tempBucketPath")) \
            .option("table", config.get("DEFAULT", "targetTableName")) \
            .option("credentials", "credentials") \
            .option("project", config.get("DEFAULT", "gcpProjectId")) \
            .save()