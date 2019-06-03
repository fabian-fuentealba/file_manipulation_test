#!/usr/bin/env python

import urllib.request
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

def spark_obj( app_name ):

    try:
        spark = SparkSession.builder.appName( app_name ).config("spark.some.config.option", "some-value").getOrCreate()
        return { 'type': True, 'msg': 'Objeto creado con exito ...', 'obj': spark }
    except Exception as e:
        return { 'type': False, 'msg': e, 'obj': None }


def load( df, path ):

    try:
        df.coalesce(1).write.format("orc").save(path=path,mode='overwrite')
        return { 'type': True, 'msg': "Archivo exitosamente creado en %s ..." % path , 'obj': None }
    except Exception as e:
        return { 'type': False, 'msg': e, 'obj': None }


def transform( df ):

    try:
        schema = StructType([StructField('latitude',StringType(),True), StructField('longitude',StringType(),True) ])
        d_f = df.withColumn("Location", from_json("Location", schema)).select(col('County'),col('License Number'),col('Operation Type'),col('Establishment Type'),col('Entity Name'),col('DBA Name'),col('Street Number'),col('Street Name'),col('Address Line 2'),col('Address Line 3'),col('City'),col('State'),col('Zip Code'),col('Location.*'))
        return { 'type': True, 'msg': "Archivo exitosamente transformado ...", 'obj': d_f }
    except Exception as e:
        return { 'type': False, 'msg': e, 'obj': None }


def extract( url, spark ):
   
    try:        
        sc = spark.sparkContext                    
        file = urllib.request.urlopen(url).read().decode('utf-8')
        df = spark.read.option("header", "true").option("delimiter", "|").csv(sc.parallelize(file.splitlines()))
        return { 'type': True, 'msg': "Archivo exitosamente extraido de %s ..." % url , 'obj': df }                
    except Exception as e:                
        return { 'type': False, 'msg': e, 'obj': None }        



def main():

    url = "https://s3.amazonaws.com/rrhh-test/clean_test.csv"
    path_orc_destination = '/home/adminroot/bin/test/file_orc'
    sp = spark_obj("TEST_DE")
    
    if sp['type'] == True:

        spark = sp['obj']
        Logger= spark._jvm.org.apache.log4j.Logger
        mylogger = Logger.getLogger(__name__)
        ex = extract( url, spark )

        if ex['type'] == True:
            mylogger.info( ex['msg'] )
            tr = transform( ex['obj'] )
            if tr['type'] == True:
                mylogger.info( tr['msg'] )
                lo = load( tr['obj'], path_orc_destination )           
                mylogger.info( lo['msg'] )
            else:
                mylogger.error( tr['msg'] )
        else:
            mylogger.error( ex['msg'] )
    else:
        print( sp['msg'] )
            
            
if __name__ == "__main__":

    main()