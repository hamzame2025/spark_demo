import unittest
from pipeline import transform
from pyspark.sql import SparkSession
from pyspark.sql.types import *

class TransformTest(unittest.TestCase):
    def test_transform_should_replace_null_value (self):
        spark = SparkSession.builder \
            .appName("testing app") \
            .enableHiveSupport().getOrCreate()

        myschema = StructType([
            StructField('Age', IntegerType()),
            StructField('Salary', FloatType()),
            StructField('Gender', StringType()),
            StructField('Country', StringType()),
            StructField('Purchased', StringType())
        ])

        df = spark.read.csv("mock_retailstore.csv", header=True, schema=myschema)

        df.show()

        tranform_process = transform.Transform(spark)
        transformed_df = tranform_process.transform_data(df)
        transformed_df.show()
        mean_salary = df.groupBy().avg('Salary').take(1)[0][0]
        new_salary = transformed_df.filter("Age = 2").select("new_salary").collect()[0][0]

        print("new salary is " + str(new_salary))

        self.assertEqual(mean_salary, new_salary)


if __name__ == '__main__':
    unittest.main()



