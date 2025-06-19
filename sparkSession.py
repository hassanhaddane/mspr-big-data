from pyspark.sql import SparkSession


def start_sparkSession():

    spark = SparkSession\
    .builder\
    .master('local')\
    .appName('food_data_pipeline') \
    .config("spark.driver.extraClassPath", "C:/jar/postgresql-42.6.0.jar") \
    .getOrCreate()

    print("Spark object is created...")

    return spark



