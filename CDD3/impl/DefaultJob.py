from CDD3.iface.iJob import iJob
import CDD3.factory.fileReaderFactory as readerFactory
import CDD3.factory.fileWriterFactory as filewriterFactory
import CDD3.factory.tablewriterFactory as tablewriterFactory

from CDD3.driver.driver import config
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
        if config.get("DEFAULT","targetType").lower() == "table":
            tablewriterFactory.gettableWriter(self.config).write(df)
        elif config.get("DEFAULT","targetType").lower() == "file":
            filewriterFactory().getFileWriter(self.config).write(df)