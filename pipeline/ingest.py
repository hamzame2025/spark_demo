import pyspark
from pyspark.sql.types import IntegerType
from pyspark.sql import SparkSession
import logging
import logging.config
from pyspark.sql.types import *


class Ingest:
    logging.config.fileConfig("pipeline/resources/configs/logging.conf")

    def __init__(self,spark):
        self.spark=spark

    def ingest_data(self):
        logger = logging.getLogger("Ingest")
        logger.info('Ingesting from csv')

        myschema = StructType([
            StructField('Age', IntegerType()),
            StructField('Salary', FloatType()),
            StructField('Gender', StringType()),
            StructField('Country', StringType()),
            StructField('Purchased', StringType())
        ])

        customer_df = self.spark.read.csv("pipeline/retailstore.csv",header=True,schema=myschema)
        logger.info('DataFrame created')
        logger.warning('DataFrame created with warning')
        return customer_df






