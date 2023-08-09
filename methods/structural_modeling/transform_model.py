#! /usr/bin/env python 

from ..utils import Utils

utils = Utils()
app_name = "Transform model"

class Transformation:
    @staticmethod
    def trans_for_json(df):
        spark_session = utils.create_spark_session(app_name)
        # Transformar o DataFrame em JSON
        json_df = df.toJSON()
        return json_df
