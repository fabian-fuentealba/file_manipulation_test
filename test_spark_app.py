#!/usr/bin/env python

import unittest
import spark_app
from pyspark.sql import SparkSession

class TestMyModule(unittest.TestCase):

    def test_spark_obj( self ):
        self.assertEqual(spark_app.spark_obj('TEST')['type'], True )

    def test_extract( self ):
        url = "https://s3.amazonaws.com/rrhh-test/clean_test.csv"
        spark = spark_app.spark_obj("TEST_DE")['obj']
        self.assertEqual(spark_app.extract( url, spark )['type'], True )

    def test_extract_error( self ):
        url = ""
        spark = spark_app.spark_obj("TEST_DE")['obj']
        self.assertEqual(spark_app.extract( url, spark )['type'], False )       



if __name__ == "__main__":

    unittest.main()