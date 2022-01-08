from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,IntegerType,BooleanType,DoubleType
import yaml
import os.path

if __name__ == '__main__':
    #Create SparkSession
    spark=SparkSession\
        .builder\
        .appName("Read CSV files")\
        .config('spark.jars.pakages','org.apache.hadoop:hadoop-aws:2.7.4')\
        .getOrCreate()
    spark.sparkContext.setLogLevel('Error')

    current_dir=os.path.abspath(os.path.dirname(__file__))
    app_config_path=os.path.abspath(current_dir+"/../../../"+"application.yml")
    app_secrets_path=os.path.abspath(current_dir+"/../../../"+".secrets")

    conf=open(app_config_path)
    app_conf=yaml.load(conf,Loader=yaml.FullLoader)
    secret=open(app_secrets_path)
    app_secret=yaml.load(secret,Loader=yaml.FullLoader)

    hadoop_conf=spark.SparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", app_secret["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", app_secret["s3_conf"]["secrets_access_key"])

    print("\n Creating dataframe ingestion CSV file using SparkSession.read.format()")
    #Create Schema
    fin_schema=StructType()\
        .add("id", IntegerType(), True)\
        .add("has_debt"), BooleanType(), True)\
        .add("has_financial_dependents", BooleanType(), True)\
        .add("has_student_loans", BooleanType(), True)\
        .add("income", DoubleType(), True)
    #Create dataframe-process 1
    fin_df=spark.read\
        .option("header","false")\
        .option("delimiter",",")\
        .format("csv")\
        .schema(fin_schema)\
        .load("s3a://"+app_conf["s3_conf"]["s3_bucket"]+"/finances.csv")

    fin_df.printSchema()
    fin_df.show()
    #Create dataframe -process 2
    finance_df=spark.read\
        .option("mode","DROPMALFORMED")\  #3modes-PERMISSIVE,FAILFAST,dropmalformed
        .option("header","false")\
        .option("delimiter",",")\
        .option("inferSchema","true")
        .csv("s3a://"+app_conf["s3_conf"]["s3_bucket"]+"/finances.csv") \
        .toDF("id", "has_debt", "has_financial_dependents", "has_student_loans", "income")

    print("Number of partitions:" + str(fin_df.rdd.getNumPartitions()))
    finance_df.printSchema()
    finance_df.show()

    finance_df\
        .repartition(2)\
        .write\
        .partitionBy("id")\
        .mode("overwrite")\  #ovrwrite,append,errorifexists,
        .option("header","true")\
        .option("delimeter","~")\
        .csv("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/fin")

    spark.stop()


