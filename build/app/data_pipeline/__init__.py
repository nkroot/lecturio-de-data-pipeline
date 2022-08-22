from google_drive_downloader import GoogleDriveDownloader as gdd
from zipfile import ZipFile
from pathlib import Path
from shared.job_base import JobBase
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DateType
from pyspark.sql.functions import input_file_name,col,regexp_extract,to_date,coalesce,to_timestamp,from_utc_timestamp,to_utc_timestamp,concat,lit
from os import listdir,remove
from shared.common import Shared
from shared.time_delta import timeit
from typing import Any, Dict,List

class DataRunner(JobBase):

    def __init__(self):
        spark = None
        super(DataRunner, self).__init__(spark)
        self._check_and_Set_params()

    
    def _check_and_Set_params(self):

        self.common = Shared()
    
    def list_csv_files(self,path_to_dir,suffix = ".csv") -> List:

        csv_files = listdir(path_to_dir)

        return [file_name for file_name in csv_files if file_name.endswith(suffix)]

    @timeit
    def data_acquisation(self,config:Dict) -> None :

        gdd.download_file_from_google_drive(file_id=config['file_id'],
                                        dest_path=config['raw_data_path'],
                                        unzip=True)
        remove(config['raw_data_path'])

        source_data_path = Path(config['source_data_path'])

        for zip_file_name in source_data_path.glob('*.zip'):
            with ZipFile(zip_file_name, 'r') as archive:
                archive.extractall(path=config['source_data_path'])
                self.common.log(f"""Unzipping...Done for {zip_file_name.stem}""")
            remove(f"{zip_file_name}")

    # @timeit
    def data_mapping (self,config:Dict):

        
        self.csv_file_list = self.list_csv_files(config['source_data_path'])

        schema = StructType([
            StructField('order_no',IntegerType(),True),
            StructField('order_date',StringType(),True),
            StructField('order_time',StringType(),True),
            StructField('transaction_type',StringType(),True),
            StructField('product_title',StringType(),True),
            StructField('currency',StringType(),True),
            StructField('amount',StringType(),True),
            StructField('file_name',StringType(),True),
            
        ])

        raw_data_path = f"{config['source_data_path']}/*[0-9].csv"
        #self.common.log(f"raw data path ->{raw_data_path}",level='info')

        df_raw_file_data= self.spark.read.csv(path = raw_data_path,
                                             sep = ',',
                                             header=True,inferSchema=True)\
                                            .withColumn("file_name",input_file_name())
        
        #self.common.log(f"Count of historic data from Dataframe :{df_raw_file_data.count()}",level='info')
        
        self.common.print_msg(df_raw_file_data.count())

        raw_data_current_month_path = f"{config['source_data_path']}/*[^0-9].csv"

        df_raw_file_data= df_raw_file_data.unionAll (self.spark.read.csv(path = raw_data_current_month_path,                                            
                                             sep = ',',
                                             header=True,inferSchema=True)\
                                            .withColumn("file_name",input_file_name()))
        
        # self.common.log(f"Count of current and historic data from Dataframe :{df_raw_file_data.count()}",level='info')
       
        # self.common.log(df_raw_file_data.printSchema(),level='info')
        
        self.df_mapped_data = self.spark.createDataFrame(df_raw_file_data.collect(),schema)
        # self.common.log(df_mapped_data.printSchema(),level='info')
        self.common.print_msg(self.df_mapped_data.count())

        return self.df_mapped_data

        
    # @timeit
    def data_transformation(self):
        
        def dynamic_date(cola, frmts=("MM-dd-yyyy", "yyyy-MM-dd","MMMM dd, yyyy","MMM d, yyyy")):
            return coalesce(*[to_date(cola, i) for i in frmts])
                
        df_transformed_data = self.df_mapped_data.withColumn("flatrate",regexp_extract(self.df_mapped_data["product_title"],"Medical|Nursing",idx=0))\
                                            .withColumn("date_time",coalesce(from_utc_timestamp(to_utc_timestamp(to_timestamp(self.df_mapped_data['order_time']/1.0),'PST'),"CET"),
                                                        to_timestamp(concat(dynamic_date(self.df_mapped_data["order_date"]),lit(' '),col('order_time')),'yyyy-MM-dd hh:mm:ss aa Z'),
                                                        ))\
                                            .filter((self.df_mapped_data.transaction_type.isin("Charge","Charged")))                                        
                                            
                                            
        self.df_transformed_data = df_transformed_data.select("order_no","date_time","flatrate","product_title","currency","amount","transaction_type")\
                                                .filter(df_transformed_data.flatrate.isin("Medical","Nursing"))
                                                
        return self.df_transformed_data

    @timeit
    def data_loading(self):
        
        self.common.save_as_parquet(self.df_transformed_data,'sales_transaction','flatrate')



    def execute(self):
        
        self.data_acquisation(self.common.config)
        self.data_mapping(self.common.config)
        self.data_transformation()
        self.data_loading()