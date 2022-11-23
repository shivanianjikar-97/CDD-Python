from CDD2.iface.iWriter import writer
from CDD2.driver.driver import config

class bqWriter(writer):

    def write(self, df):
        df.write.format("bigquery") \
            .option("temporaryGcsBucket", config.get("DEFAULT", "tempBucketPath")) \
            .option("table", config.get("DEFAULT", "targetTableName")) \
            .option("credentials", "credentials") \
            .option("project", config.get("DEFAULT", "gcpProjectId")) \
            .save()