#! /usr/bin/env python 

from ..utils import Utils

utils = Utils()
app_name = "Reader file"

class FileReader:
    def __init__(self, app_name):
        self.spark_session = utils.create_spark_session(app_name)

    def read(self, file_path):
        raise NotImplementedError

# read .csv file
class CsvReader(FileReader):
    def read(self, file_path):
        return self.spark_session.read.csv(file_path)

# read text file
class TxtReader(FileReader):
    def read(self, file_path):
        return self.spark_session.read.text(file_path)

# read json file
class JsonReader(FileReader):
    def read(self, file_path):
        return self.spark_session.read.json(file_path)

# read parquet file
class ParquetReader(FileReader):
    def read(self, file_path):
        return self.spark_session.read.parquet(file_path)

# read orc file
class OrcReader(FileReader):
    def read(self, file_path):
        return self.spark_session.read.orc(file_path)

# read avro file
 
# instaling plugins for using avro file (verify version)
# ./bin/spark-shell --packages org.apache.spark:spark-avro_version
class AvroReader(FileReader):
    def read(self, file_path):
        return self.spark_session.read.format("avro").load(file_path)

# read delta file
 
# instaling plugins for using delta file (verify version)
# ./bin/spark-shell --packages io.delta:delta-core_version
class DeltaReader(FileReader):
    def read(self, file_path):
        return self.spark_session.read.format("delta").load(file_path)