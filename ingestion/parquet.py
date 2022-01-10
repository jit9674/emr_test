from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,IntegerType,DoubleType,BooleanType
import os
import yaml

if __init__="__main__":
    #Create SparkSession
    spark=SparkSession\
        .builder\
        .appName("parquet reader")\
        .config('spark.jars.packages','org.apache.hadoop:hadoop-aws:2.7.4')\
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    current_dir=os.path.abspath(os.path(__file__))
    app_config_path=os.path.abspath(current_dir + "/../../../" + "application.yaml")
    app_secrets_path=os.path.abspath(current_dir + "/../../../" + ".secrets")

    conf=open(app_config_path)
    app_conf=yaml.load(conf, Loader=yaml.FullLoader)
    secret=open(app_secrets_path)
    app_secrets=yaml.load(secret, Loader=yaml.FullLoader)

    hadoop_conf=spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", app_secrets["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", app_secrets["s3_conf"]["secret_access_key"])

    print("\n Creating a dataframe to read parquet file using 'SparkSession.read.parquet()'")
    nyc_omo_df=spark.read\
        .parquet("s3a://" + app_conf["s3_conf"]["s3_bucket"] +"/NYC_OMO")\
        .repartition(5)

    print("# of records=" + str(nyc_omo_df.count()))
    print("# of records=" + str(nyc_omo_df.rdd.getNumPartitions()))

    nyc_omo_df.printSchema()

    print("Summary of NYC Open Market Order(OMO) charges dataset")
    nyc_omo_df.describe().show()

    print("OMO frequency distribution of different boroughs")
    nyc_omo_df.groupby("Boro")\
        .agg({Boro})
