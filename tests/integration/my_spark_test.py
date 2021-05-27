from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType


def test_spark():
    data = [('Category A', 100, "This is category A"),
            ('Category B', 120, "This is category B"),
            ('Category C', 150, "This is category C")]

    app_name = "PySpark Example - Python Array/List to Spark Data Frame"
    master = "local"

    # Create Spark session
    spark = SparkSession.builder \
        .appName(app_name) \
        .master(master) \
        .getOrCreate()

    schema = StructType([
        StructField('Category', StringType(), True),
        StructField('Count', IntegerType(), True),
        StructField('Description', StringType(), True)
    ])

    # Convert list to RDD
    rdd = spark.sparkContext.parallelize(data)

    # Create data frame
    df = spark.createDataFrame(rdd, schema)
    count = df.count()
    assert count == 3
