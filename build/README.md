# Spark Application on Docker

## Introduction

This pipeline built from the google drive files to target parquet files and tech stack is **Apache Spark** cluster in standalone mode with a **JupyterLab** interface built on top of **Docker**.


### Build from your local machine

> **Note**: Local build is currently only supported on Linux OS distributions.

1. Place the source code in a path.
2. Move to the build directory.

```bash
cd build
```

3. Edit the [build.yml](build/build.yml) file with required tech stack version.
4. Match those version on the [docker compose](build/docker-compose.yml) file.
5. Build up the images by running following command in git bash or any linux flavour terminal.

```bash
chmod +x build.sh ; ./build.sh
```

6. Start the cluster.

```bash
docker-compose up
```

7. Stop the cluster by typing `ctrl+c` on the terminal;
8. Run step 6 to restart the cluster.
9. If the images are thrown error please use below command to create

```bash
docker-compose up -d --force-recreate
```

## <a name="tech-stack"></a>Tech Stack

- Infra

| Component      | Version |
| -------------- | ------- |
| Docker Engine  | 1.13.0+ |
| Docker Compose | 1.10.0+ |

- Languages and Kernels

| Spark | Hadoop | Python | [Python Kernel](https://ipython.org/) |
| ----- | ------ | ------- | ---------------------------------- | ------ | ------------------------------------- |
| 3.1.3   | 3.2    | 3.7.3  | 7.19.0                                 |
| 2.x   | 2.7    | 3.7.3  | 7.19.0                                 |

- Apps

| Component      | Version                 | Docker Tag                                           |
| -------------- | ----------------------- | ---------------------------------------------------- |
| Apache Spark   | 2.4.0 \| 2.4.4 \| 3.0.0 | **\<spark-version>**                                 |
| JupyterLab     | 2.1.4 \| 3.0.0          | **\<jupyterlab-version>**-spark-**\<spark-version>** |


## Cluster overview
> **Note**: Try to connect jupyter lab and others using following Ips .

| Application     | URL                                      | Description                                                |
| --------------- | ---------------------------------------- | ---------------------------------------------------------- |
| JupyterLab      | [localhost:8888](http://localhost:8888/) | Cluster interface with built-in Jupyter notebooks          |
| Spark Driver    | [localhost:4040](http://localhost:4040/) | Spark Driver web ui                                        |
| Spark Master    | [localhost:8080](http://localhost:8080/) | Spark Master node                                          |
| Spark Worker I  | [localhost:8081](http://localhost:8081/) | Spark Worker node with 1 core and 512m of memory (default) |
| Spark Worker II | [localhost:8082](http://localhost:8082/) | Spark Worker node with 1 core and 512m of memory (default) |

## Sample Code
> **Note**: Run the below code in the jupyter lab to check the table structure.
```bash
from pyspark.sql import SparkSession

session = SparkSession\
                        .builder\
                        .appName("Data retrival from parquet")\
                        .master("spark://spark-master:7077")\
                        .config("spark.executor.memory", "1G")\
                        .enableHiveSupport()\
                        .config("hive.default.fileformat","parquet") \
                        .getOrCreate()
spark = session.newSession()

spark.sparkContext.setLogLevel('WARN') 
spark.conf.set("hive.default.fileformat","parquet")

spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

def _getCachePath(hdfs_path, table_name):
    '''
        get filepath to parquet
    '''
    return "{}/{}.parquet".format(hdfs_path,table_name)

def readFromParquet(hdfs_path,table_name):
    return spark.read.parquet(_getCachePath(hdfs_path,table_name))

df_sales = readFromParquet('/opt/workspace/data/target_wh','sales_transaction')
df_sales.select("order_no","date_time","flatrate","product_title","currency","amount","transaction_type").show(truncate = False)
```