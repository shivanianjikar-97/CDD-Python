import CDD2.impl.DefaultJob as DefaultJob

class jobFactory:
    def getJob(self,config,spark):
        return DefaultJob(config,spark)