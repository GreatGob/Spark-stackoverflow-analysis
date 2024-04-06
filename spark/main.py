import logging
import re
from datetime import datetime
from typing import Optional

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

logging.basicConfig(format="%(levelname)s: %(message)s", level=logging.INFO)


def extract_programming_languages(title: str) -> Optional[list]:
    programming_languages = [
        "Java",
        "Python",
        "C++",
        "C#",
        "Go",
        "Ruby",
        "Javascript",
        "PHP",
        "HTML",
        "CSS",
        "SQL",
    ]

    languages_list = []

    for language in programming_languages:
        regex = r"\b" + re.escape(language) + r"(?=\W|$)"
        pattern = re.compile(regex, re.IGNORECASE)
        matches = pattern.search(title.lower())
        if matches:
            languages_list.append(language)
    return languages_list


def extract_domains(body: str) -> Optional[list]:
    if body:
        regex = r"https?://([a-zA-Z0-9.-]+)"
        pattern = re.compile(regex)
        matches = pattern.findall(body)
        if matches:
            return matches


if __name__ == "__main__":
    RUNTIME_VERSION = "3.0.0"
    DATABASE_NAME = "stack_overflow"
    MONGO_SOURCE = "com.mongodb.spark.sql.DefaultSource"
    MONGO_URI = f"mongodb://admin:admin@mongo:27017/{DATABASE_NAME}?authSource=admin"
    MONGO_SPARK_PACKAGE = (
        f"org.mongodb.spark:mongo-spark-connector_2.12:{RUNTIME_VERSION}"
    )
    DATA_DIR = "/usr/local/share/data/"

    conf = (
        SparkConf()
        .setAppName("DEP303x_ASM1")
        .set("spark.jars.packages", MONGO_SPARK_PACKAGE)
        .set("spark.mongodb.input.uri", MONGO_URI)
        .set("spark.mongodb.output.uri", MONGO_URI)
    )

    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")

    spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()

    question_schema = StructType(
        [
            StructField("OwnerUserId", IntegerType(), nullable=True),
            StructField("CreationDate", DateType(), nullable=True),
            StructField("ClosedDate", DateType(), nullable=True),
            StructField("Score", IntegerType(), nullable=True),
            StructField("Title", StringType(), nullable=True),
            StructField("Body", StringType(), nullable=True),
        ]
    )

    questions_df = (
        spark.read.format(MONGO_SOURCE)
        .option("uri", MONGO_URI)
        .option("collection", "questions")
        .load()
    )

    logging.info("Question schema structure")
    questions_df.printSchema()

    questions_df = (
        questions_df.withColumn("OwnerUserId", F.col("OwnerUserId").cast("integer"))
        .withColumn("CreationDate", F.to_date("CreationDate"))
        .withColumn("ClosedDate", F.to_date("ClosedDate"))
    )

    logging.info("Question data frame")
    questions_df.show(20)

    # Registering functions
    extract_languages_udf = F.udf(
        extract_programming_languages, returnType=ArrayType(StringType())
    )
    extract_domains_udf = F.udf(extract_domains, returnType=ArrayType(StringType()))

    """
    TODO: Requirement 1: Calculate the number of occurrences of programming languages
    """
    programming_language_df = (
        questions_df.withColumn("extract_languages", extract_languages_udf("Title"))
        .withColumn("Programming Language", F.explode("extract_languages"))
        .groupBy("Programming Language")
        .agg(F.count("*").alias("Count"))
        .orderBy("Count")
    )
    logging.info("Requirement 1")
    programming_language_df.show(20)

    """
    TODO: Requirement 2: Find the most used domains in the questions
    """
    domain_df = (
        questions_df.withColumn("extract_domain", extract_domains_udf("Body"))
        .withColumn("Domain", F.explode("extract_domain"))
        .groupBy("Domain")
        .agg(F.count("*").alias("Count"))
        .orderBy("Count")
    )
    logging.info("Requirement 2")
    domain_df.show(20)

    """
    TODO: Requirement 3: Calculate the total score of the User for each day

    """
    score_total_window = (
        Window.partitionBy("OwnerUserId")
        .orderBy("CreationDate")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    score_df = (
        questions_df.select("OwnerUserId", "CreationDate", "Score")
        .withColumn("TotalScore", F.sum("Score").over(score_total_window))
        .drop("Score")
    )
    logging.info("Requirement 3")
    score_df.show(20)

    """
    TODO: Requirement 4: Calculate the total number of points achieved by the User in a period of time
    """
    START_DATE = "01-01-2008"
    END_DATE = "01-01-2009"
    start_date = datetime.strptime(START_DATE, "%d-%m-%Y")
    end_date = datetime.strptime(END_DATE, "%d-%m-%Y")
    date_df = (
        questions_df.select("CreationDate", "OwnerUserId", "Score")
        .where(
            (F.col("CreationDate").between(start_date, end_date))
            & (F.col("OwnerUserId").isNotNull())
        )
        .drop(F.col("CreationDate"))
    )
    day_score_df = (
        date_df.groupBy(F.col("OwnerUserId"))
        .agg(F.sum(F.col("Score")).alias("TotalScore"))
        .sort(F.col("OwnerUserId"))
    )
    logging.info("Requirement 4")
    day_score_df.show(20)

    """
    TODO: Requirement 5: Find questions with multiple answers
    """
    answers_schema = StructType(
        [
            StructField("OwnerUserId", IntegerType(), nullable=True),
            StructField("CreationDate", DateType(), nullable=True),
            StructField("ParentId", IntegerType(), nullable=True),
            StructField("Score", IntegerType(), nullable=True),
            StructField("Body", StringType(), nullable=True),
        ]
    )
    answers_df = (
        spark.read.format(MONGO_SOURCE)
        .option("uri", MONGO_URI)
        .option("collection", "answers")
        .load()
    )
    logging.info("Answer schema structure")
    answers_df.printSchema()

    answers_df = (
        answers_df.withColumn("OwnerUserId", F.col("OwnerUserId").cast("integer"))
        .withColumn("ParentId", F.col("ParentId").cast("integer"))
        .withColumn("CreationDate", F.to_date("CreationDate"))
    )
    logging.info("Answer data frame")
    answers_df.show(20)

    logging.info("Create new database for bucket join")
    spark.sql("CREATE DATABASE IF NOT EXISTS NEW_DB")
    spark.sql("USE NEW_DB")

    questions_df.coalesce(1).write.bucketBy(10, "Id").format("parquet").mode(
        "overwrite"
    ).option("path", "{}/question_data".format(DATA_DIR)).saveAsTable(
        "NEW_DB.question_df2"
    )

    answers_df.coalesce(1).write.bucketBy(10, "ParentId").format("parquet").mode(
        "overwrite"
    ).option("path", "{}/answer_data".format(DATA_DIR)).saveAsTable("NEW_DB.answer_df2")
    logging.info("Created successfully")

    logging.info("Read new databases")
    new_question_df = spark.read.table("NEW_DB.question_df2")
    new_answer_df = spark.read.table("NEW_DB.answer_df2")

    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

    new_question_df = (
        new_question_df.withColumnRenamed("Id", "question_id")
        .withColumnRenamed("OwnerUserId", "owner_user_id")
        .withColumnRenamed("Score", "question_score")
        .withColumnRenamed("CreationDate", "question_creation_date")
    )

    join_expr = new_question_df.question_id == new_answer_df.ParentId
    join_df = new_question_df.join(new_answer_df, join_expr, "inner")

    join_df_count = (
        join_df.select("question_id", "Id")
        .groupBy("question_id")
        .agg(F.count("Id").alias("Number of answers"))
        .sort(F.asc("question_id"))
        .filter(F.col("Number of answers") > 5)
    )

    logging.info("Requirement 5")
    join_df_count.show()

    spark.stop()
