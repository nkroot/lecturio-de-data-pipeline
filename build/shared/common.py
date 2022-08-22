from datetime import datetime
from configparser import ConfigParser
import time
import logging
from typing import Dict


class Shared(object):

    def __init__ (self):

        self.config = self.read_config("system_variables")

    def log(self,msg, level='debug'):
        '''
            adds log message to logger with formatting
        '''
        logging.basicConfig(filename="metadata_log_file.log",
                        format='%(asctime)s %(message)s',level=logging.DEBUG
                        ,filemode='a'
                        )

        logger = logging.getLogger()
        
        if level=='info':
            l = logger.info 
        elif level=='error':
            l = logger.error
        else :
            l = logger.debug

        l(msg)

    

    #Printing a message with time stamp
    def print_msg(self,msg:str)-> None:
        '''
        prints message with time stamp
        
        '''
        time_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print('%s: %s' % (time_str, msg))   

    def _getCachePath(self,hdfsPath,table_name):
            '''
                get filepath to parquet
            '''
            return "{}/{}.parquet".format(hdfsPath,table_name)

    def save_as_parquet(self,df, table_name,partion_by_column_name=None,mode="overwrite"):
        try:    
            '''
                save df as parquet and return df of file
            '''
            start_time = time.time()
            mode = mode.lower()
            partition_count = df.rdd.getNumPartitions()

            if partition_count>500:
                self.print_msg("atempting to write %d number of partitions, resetting to 50 partitions."%(partition_count))
                df = df.repartition(50)
            
            if mode in ['overwrite', 'append']:
                hdfs_path = self.config["target_path"]
                f = self._getCachePath(hdfs_path,table_name)
                df.write.parquet(f,mode=mode)
                # Available codecs are brotli, uncompressed, lz4, gzip, lzo, snappy, none, zstd,zlib
                if partion_by_column_name is not None:
                    df.repartition(partition_count,partion_by_column_name).write.partitionBy(partion_by_column_name).option("compression", "snappy").parquet(f,mode=mode)
                else:
                    df.coalesce(partition_count).write.option("compression", "ZLIB").parquet(f,mode=mode)
                                
            else:
                raise RuntimeError("unknown write mode: %s"%(mode))
            
            endTime = time.time()
            elapSecs = int(endTime - start_time)
            self.print_msg("Wrote to file {0} in {1} secs".format(f, str(elapSecs)))
        except Exception as e:
            self.log(str(e),'error')

    def read_config(self,section:str) -> Dict:

        try:

            step = 1 
            config_parser = ConfigParser()
            step = 2
            config_parser.read("./shared/config.cfg")
            step = 3 
            conf = {}

            if config_parser.has_section(section):

                params = config_parser.items(section)

                for param in params:
                    conf[param[0]] = param[1]
            return conf
        except Exception as e:
            self.print_msg(f'Fatal Error in read_config : section= {section} step = {step}')
            self.print_msg(str(e))
            raise Exception(e)

