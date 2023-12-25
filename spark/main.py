from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .master('local[*]') \
        .appName('DEP303x_ASM1') \
        .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
        .config('spark.mongodb.input.uri', 'mongodb://127.0.0.1/dep303x.questions') \
        .config('spark.mongodb.output.uri', 'mongodb://127.0.0.1/dep303x.questions') \
        .getOrCreate()

    schema = """
        Id Int,
        OwnerUserId Int,
        CreationDate Date,
        ClosedDate Date,
        Score Int,
        Title String,
        Body String
        """

    df = spark.read \
        .format('com.mongodb.spark.sql.DefaultSource') \
        .option('uri', 'mongodb://127.0.0.1/dep303x.questions') \
        .schema(schema) \
        .load()

    df.printSchema()

    df.show(5)

    spark.stop()
