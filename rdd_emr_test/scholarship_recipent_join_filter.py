from pyspark.sql import SparkSession ,Row
from disutils.util import strtobool
import os.path
import yaml

if__name__=='__main__':

    os.environ["PYSPARK_SUBMIT_ARGS"]=(
            '--packages "org.apache.hadoop" % "hadoop-aws" % "2.7.4" pyspark-shell'
    )

    #Create the SparkSession
    spark=SparkSession\
        .builder\
        .appName("RDD project")\
        .master('Local[*]')\
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    current_dir=os.path.abspath(os.path.dirname(__file__))
    app_config_path=os.path.abspath(current_dir+"/../"+ "application.yml")
    app_secrets_path=os.path.abspath(current_dir+"/../"+ ".secrets")

    conf=open(app_config_path)
    app_conf=yaml.load(conf,Loader=yaml.FullLoader)
    secret=open(app_secrets_path)
    app_secret=yaml.load(secret,Loader=yaml.FullLoader)

    #setup spark to s3
    hadoop_conf=spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key",app_secret["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key",app_secret["s3_conf"]["secret_access_key"])



