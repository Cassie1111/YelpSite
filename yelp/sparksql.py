from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import *
from pyspark.ml import PipelineModel
from pyspark.sql.types import *
from pyspark.ml.classification import LogisticRegressionModel

class SparkSql(object):
    def __init__(self):
        self.spark = SparkSession.builder \
            .master('yarn') \
            .appName("Yelp Online Testing") \
            .getOrCreate()

        self.lda_model = PipelineModel.load('hdfs:///project/small_data/lda_model_10')
        self.lr_model = LogisticRegressionModel.load('hdfs:///project/small_data/lr-model-10')



