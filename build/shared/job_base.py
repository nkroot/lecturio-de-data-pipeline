from pyspark.sql import SparkSession
from os import listdir

class JobBase(object):

    def __init__(self,spark):

        self.spark = spark
        
        if self.spark is None:

            self.session = SparkSession\
                        .builder\
                        .appName("Data pipeline from csv files to parquet")\
                        .config("spark.master", "local")\
                        .config("spark.executor.memory", "1G") \
                        .config("hive.default.fileformat","parquet") \
                        .getOrCreate()
                        
            self.spark = self.session.newSession()
            
        self.spark.sparkContext.setLogLevel('WARN') 
        self.spark.conf.set("hive.default.fileformat","parquet")

        self.spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")