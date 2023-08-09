#! /usr/bin/env python 

# Spark SQL
# from pyspark.sql import SparkSession, SQLContext, HiveContext
# from pyspark.sql.functions import col, lit, when
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType
 
# Spark MLlib - Machine Learning
# from pyspark.ml.feature import VectorAssembler, StringIndexer
# from pyspark.ml.classification import LogisticRegression
# from pyspark.ml.regression import LinearRegression
# from pyspark.ml.clustering import KMeans
# from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# Spark Streaming
# from pyspark.streaming import StreamingContext


class Utils:
    def __init__(self):
        self.limit = 100
        self.hc = None

# Imports framework Spark and create spark session--
    def create_spark_session(self, app_name):
        try:
            import pyspark
            from pyspark import SparkConf, SparkContext
            from pyspark.sql import SparkSession, SQLContext, HiveContext
            from pyspark.sql.functions import count, when, udf
            from pyspark.sql.types import StringType

            sc = SparkContext()
            self.hc = HiveContext(sc)
            spark = SparkSession.builder \
                .appName(app_name) \
                .getOrCreate()
            return spark
        except ImportError:
            print("Pyspark is not available in this environment.")
            return None

    def hive(self, df):
        return self.hc.sql(df)

    def tmp_view(self, df, name):
        return df.repartition(1).registerTempTable("{0}".format(name))

    def visualizer_df(self, df):
        df.show(self.limit, truncate=False)

    def counter(self, df):
        counter = df.agg(count('*'))
        analyse_counter = counter\
            .withColumn("analyseCounter", when(counter["count(1)"] == "0", "no record selected")
            .otherwise("records successfully selected")) 
        return analyse_counter.show(truncate=False)   

    def set_hive_conf(self, queue):
        self.hc.sql(""" set hive.exec.max.dynamic.partitions.pernode=10000; """)
        self.hc.sql(""" set hive.exec.max.dynamic.partitions=100000; """)
        self.hc.sql(""" set hive.merge.smallfiles.avgsize = 134217728; """)
        self.hc.sql(""" set hive.exec.dynamic.partition=true; """)
        self.hc.sql(""" set hive.exec.dynamic.partition.mode=nonstrict; """)
        self.hc.sql(""" set hive.merge.mapfiles = true; """)
        self.hc.sql(""" set tez.queue.name="{0}"; """.format(queue))

# Imports framework Pandas -- next step
    def create_pandas_session(self):
        try:
            import pandas as pd
            # Seu código para inicializar o Pandas vai aqui
        except ImportError:
            print("Pandas is not available in this environment.")
            return None
