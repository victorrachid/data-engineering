#! /usr/bin/env python 

from ..utils import Utils

utils = Utils()
app_name = "Cleaning data"

class DataCleaning:
    spark_session = utils.create_spark_session(app_name)

# drop temp table
    @staticmethod
    def drop_tmp_view(name):
        DataCleaning.spark_session.catalog.dropTempView(name)

# remove duplicate 
    @staticmethod
    def rm_duplicates(df):
        return df.dropDuplicates()

# accent cleanup
    @staticmethod
    def clean_accent(df, column: str):
# instaling unicode  "pip install unidecode"        
        from unidecode import unidecode
        unidecode_udf = udf(lambda str: unidecode(str), StringType())
        df = df.withColumn(column, unidecode_udf(df[column]))
        return df

# fill in the missing values
    @staticmethod
    def fill_missing_values(df, value):
        return df.fillna(value)

# drop line missing values
    @staticmethod
    def drop_missing_values(df):
        return df.dropna()    