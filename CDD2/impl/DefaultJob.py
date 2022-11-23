from CDD2.iface.iJob import iJob
import CDD2.factory.readerFactory as readerFactory
import CDD2.factory.writerFactory as writerFactory

class DefaultJob(iJob):
    def __init__(self,config,spark):
        self.config = config
        self.spark = spark

    def execute(self) -> bool:

        #Load data from source
        df = readerFactory().getFileReader(self.config).read()

        #transform
        df = df.selectExpr(self.config.get("DEFAULT","targetColumns"))

        #store into target
        writerFactory().getFileWriter(self.config).write(df)