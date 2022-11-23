from CDD3.driver.driver import config
from CDD3.iface.iWriter import iWriter


class oracleWriter(iWriter):

    def write(self, df):
        df.write.format('jdbc').options(
            url=config.get("DEFAULT","oracleThinClient"),
            driver=config.get("DEFAULT","oracleDriver"),
            dbtable=config.get("DEFAULT","oracleDestTable"),
            user=config.get("DEFAULT","username"),
            password=config.get("DEFAULT","password"))\
            .mode('append')\
            .save()