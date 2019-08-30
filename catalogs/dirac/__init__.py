from pyspark.sql import SparkSession
import axs
from axs import Constants

class DataBase():
    spark_context = None
    spark_session = None
    catalogs = None
    
    dirac_conf = {}
    dirac_conf['spark.executor.instances'] = 16
    
    dirac_catalogs = ["allwise", "gaiadr2", "sdss"]
    
    def init_catalogs(self):
        if (self.catalogs):
            current_tables = self.catalogs.list_tables()
            for catalog_name in self.dirac_catalogs:
                try:
                    current_tables[catalog_name]
                    print("Found", catalog_name, "in AXS catalogs.")
                except KeyError as e:
                    print("Adding", catalog_name, "to AXS catalogs...")
                    try:
                        self.catalogs.import_existing_table(catalog_name, f's3a://axscatalog/{catalog_name}', num_buckets=500,
                                                            zone_height=Constants.ONE_AMIN, import_into_spark=True)
                    except AttributeError as axs_e:
                        print(axs_e)
        else:
            print("AXS catalog not instantiated.")
            return None
    
    def start(self):
        print("Starting SparkSession creation")
        print("Building SparkSession...")
        _spark_session = SparkSession.builder
        for key, value in self.dirac_conf.items():
            print(f"Setting config: {key}={value}")
            _spark_session = _spark_session.config(str(key), str(value))
        self.spark_session = _spark_session
        print("Creating SparkSession. This may take a while.")
        self.spark_session = _spark_session.enableHiveSupport().getOrCreate()
        self.spark_context = self.spark_session.sparkContext
        self.catalogs = axs.AxsCatalog(self.spark_session)
        print("Loading catalogs...")
        self.init_catalogs()
    
    def stop(self):
        if self.spark_session:
            self.spark_session.stop()
        else:
            print("Spark cluster not created.")
        
    def __init__(self):
        self.start()
        return None
        
    def load(self, table_name):
        return None
    
    def get_spark_context(self):
        if self.spark_context:
            return self.spark_context
        else:
            print("Spark cluster not created.")
            return None
    
    def get_spark_session(self):
        if self.spark_session:
            return self.spark_session
        else:
            print("Spark cluster not created.")
            return None
        
    def get_catalogs(self):
        if self.catalogs:
            return self.catalogs
        else:
            print("AXS catalog not instantiated.")
            return None

def start():
    return DataBase()

