from CDD3.iface.iJob import iJob
import CDD3.factory.fileReaderFactory as readerFactory
import CDD3.factory.fileWriterFactory as writerFactory

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