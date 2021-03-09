import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

import logging
import logging.config

class Transform:
    #logging.config.fileConfig("pipeline/resources/configs/logging.conf")

    def __init__(self,spark):
        self.spark=spark

    def transform_data(self,df):
        #logger = logging.getLogger("Transform")
        #logger.info("Transforming")
        #logger.warning("Warning in Transformer")

        # replace null salary with mean salary

        mean_salary = df.groupBy().avg('Salary').take(1)[0][0]
        df1 = df.withColumn('new_salary',when(df.Salary.isNull(), mean_salary).otherwise(df.Salary))
        df1.show()

        return df1