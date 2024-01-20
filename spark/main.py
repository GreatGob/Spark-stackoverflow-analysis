import re
from datetime import datetime
from typing import Optional

# import pandas as pd
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as f
from pyspark.sql.types import ArrayType, DateType, IntegerType, StringType


def parse_date_col(date_str: str) -> Optional[datetime.date]:
    if date_str and date_str != "NA":
        return datetime.strptime(date_str.split("T")[0], "%Y-%m-%d").date()
    else:
        return None


def parse_owner_user_id_col(owner_user_id: str) -> Optional[int]:
    if owner_user_id and owner_user_id != "NA":
        return int(owner_user_id)
    else:
        return None


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

    MONGO_URI = "mongodb://admin:admin@mongo:27017/stack_overflow?authSource=admin"
    MONGO_SPARK_PACKAGE = "org.mongodb.spark:mongo-spark-connector_2.12:3.1.0"

    spark = SparkSession.builder \
        .appName("DEP303x_ASM1") \
        .config("spark.jars.packages", MONGO_SPARK_PACKAGE) \
        .config("spark.mongodb.input.uri", MONGO_URI) \
        .config("spark.mongodb.input.collection", "questions") \
        .config("spark.mongodb.output.uri", MONGO_URI) \
        .config("spark.mongodb.output.collection", "questions") \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    questions_schema = """
        Id Int,
        OwnerUserId Int,
        CreationDate String,
        ClosedDate String,
        Score Int,
        Title String,
        Body String
        """

    questions_df = spark.read \
        .format("com.mongodb.spark.sql.DefaultSource$") \
        .schema(questions_schema) \
        .load()

    questions_df.printSchema()

    # Registering functions
    extract_date_udf = f.udf(parse_date_col, DateType())
    extract_owner_user_id_udf = f.udf(parse_owner_user_id_col, IntegerType())
    extract_languages_udf = f.udf(
        extract_programming_languages, returnType=ArrayType(StringType())
    )
    extract_domains_udf = f.udf(extract_domains, returnType=ArrayType(StringType()))

    # Transform data types of columns
    questions_df = questions_df.withColumn("OwnerUserId", f.col("OwnerUserId").cast("integer"))
    questions_df = questions_df.withColumn("CreationDate", f.to_date("CreationDate"))
    questions_df = questions_df.withColumn("ClosedDate", f.to_date("ClosedDate"))

    questions_df.show(10)

    """
    TODO: Requirement 1: Calculate the number of occurrences of programming languages
    """
    programming_language_df = questions_df \
        .withColumn("extract_languages", extract_languages_udf("Body")) \
        .withColumn("Programming Language", f.explode("extract_languages")) \
        .group("Programming Language") \
        .agg(f.count("*").alias("Count")) \
        .orderBy("Count")
    
    programming_language_df.show(10)

    # programming_language_df = extract_languages_df.select(
    #     f.explode(extract_languages_df.ProgrammingLanguages).alias(
    #         "Programming Language"
    #     )
    # )

    # programming_language_df = programming_language_df \
    #     .groupBy("Programming Language") \
    #     .agg(f.count("*").alias("Count")) \
    #     .orderBy("Count")


    # programming_language_df.show(10)

    """
    TODO: Requirement 2: Find the most used domains in the questions
    """
    # body_df = questions_df.select("Body").filter(questions_df.Body.isNotNull())

    # extract_domains_df = body_df.withColumn("Domain", extract_domains_udf("Body"))

    # domain_df = extract_domains_df.select(
    #     f.explode(extract_domains_df.Domain).alias("Domain")
    # )

    # domain_df = (
    #     domain_df.groupBy("Domain").agg(f.count("*").alias("Count")).orderBy("Count")
    # )

    # domain_df.show(10)

    """
    TODO: Requirement 3: Calculate the total score of the User for each day
    """
    # score_total_window = Window.partitionBy("OwnerUserId") \
    #     .orderBy("CreationDate") \
    #     .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    # score_df = questions_df.select("OwnerUserId", "CreationDate", "Score") \
    #     .withColumn("Total_Score", f.sum("Score").over(score_total_window)) \
    #     .drop("Score")
    
    # score_df.show()

    '''
    TODO: Requirement 4: Calculate the total number of points achieved by the User in a period of time
    '''
    # START_DATE = '01-01-2008'
    # END_DATE = '01-01-2009'
    # start_date = datetime.strptime(START_DATE, "%d-%m-%Y")
    # end_date = datetime.strptime(END_DATE, "%d-%m-%Y")
    # date_df = questions_df \
    #     .select("CreationDate","OwnerUserId", "Score") \
    #     .where(f.col("CreationDate").between(start_date, end_date)) \
    #     .drop(f.col("CreationDate"))

    # day_score_df = date_df \
    #     .groupBy(f.col("OwnerUserId")) \
    #     .agg(f.sum(f.col("Score"))) \
    #     .sort(f.col("OwnerUserId"))

    # day_score_df.show(10)

    spark.stop()
