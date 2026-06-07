from pyspark.sql import SparkSession

def create_spark_session():

    spark = SparkSession.builder \
        .appName("Messy Data Processing") \
        .getOrCreate()

    return spark


def load_data(spark):

    df = spark.read.csv(
        "data/Messy_Employee_dataset.csv",
        header=True,
        inferSchema=True
    )

    return df

print("Spark Session initialized and Data has been loaded")