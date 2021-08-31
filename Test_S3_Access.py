"""
- OverviewPyspark on Win10: https://sparkbyexamples.com/pyspark-tutorial/#pyspark-installation
    - Python3.7 nötig, sonst kommt ein Fehler Integer required but byte
    - im Pfad von Python3.7 muss Python und Python\Scripts stehen
    - SPARK_HOME, JAVA_HOME, HADOOP_HOME sind als Umgebungsvariable gesetzt
    - Wenn winutils.exe (https://github.com/steveloughran/winutils) unter %SPARK_HOME%\bin liegt,
      ist die Umgebungsvariable HADOOP_HOME nicht nötig.

- Zur Integration mit Jupyter:
    Umgebungsvariable PYTHONPATH setzen auf "%SPARK_HOME%/python;$SPARK_HOME/python/lib/py4j-<!!!VERSION-Number!!!>-src.zip;%PYTHONPATH%"

- Zum Lesen von / Schreiben auf S3a:
    In Spark-defauls (C:\Spark\spark-2.4.3-bin-hadoop2.7\conf) steht als Konfiguration:
        spark.jars.packages                com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.2
        #spark.jars.packages					com.amazonaws:aws-java-sdk-pom:1.10.34,org.apache.hadoop:hadoop-aws:2.7.3
        spark.hadoop.fs.s3a.access.key  	<XXX>
        spark.hadoop.fs.s3a.secret.key  	<XX>
        spark.hadoop.fs.s3a.fast.upload 	true
        spark.hadoop.fs.s3n.impl    org.apache.hadoop.fs.s3native.NativeS3FileSystem
        spark.hadoop.fs.s3n.awsAccessKeyId 	<XXX>
        spark.hadoop.fs.s3n.awsSecretAccessKey <XX>

        ############ see https://gist.github.com/eddies/f37d696567f15b33029277ee9084c4a0
        ############ see https://medium.com/@purmac/using-standalone-spark-with-amazon-s3-1ff72f2cf843
"""

from pyspark.sql import SparkSession

"""
import configparser
import os


config = configparser.ConfigParser()

config.read_file(open('dl.cfg')) #Normally this file should be in ~/.aws/credentials
os.environ["AWS_ACCESS_KEY_ID"] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ["AWS_SECRET_ACCESS_KEY"] = config['AWS']['AWS_SECRET_ACCESS_KEY']
os.environ["PYTHONPATH"] = "%SPARK_HOME%\python;%SPARK_HOME%\python\lib\py4j-<VERSION>-src.zip:%PYTHONPATH%"
"""


spark1 = SparkSession.builder\
                    .master('local[*]')\
                    .getOrCreate()
#.config("spark.jars.packages","org.apache.hadoop:hadoop-aws:2.7.0")\

spark2 = SparkSession.builder\
                    .master('spark://127.0.0.1:7077')\
                    .getOrCreate()


df = spark1.read.csv("s3a://udacity-dend/pagila/payment/payment.csv")
df.write.parquet("s3a://aws-emr-resources-726459035533-us-east-1/data/payment2.parquet")

df2 = spark2.read.parquet("s3a://aws-emr-resources-726459035533-us-east-1/data/payment2.parquet")
