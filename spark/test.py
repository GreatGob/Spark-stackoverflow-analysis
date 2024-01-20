from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
import os
import re 
from datetime import datetime


MongoURI = "mongodb://admin:admin@mongo:27017/stack_overflow?authSource=admin"
Databases = "stackoverflow" 
DataDir = "/usr/local/share/data"
OutputDir = "output"

if __name__ == "__main__":
    # Tạo một Spark Session
    spark = SparkSession.builder \
        .appName("StackOverflow") \
        .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
        .config("spark.mongodb.read.connection.uri", MongoURI) \
        .config("spark.mongodb.write.connection.uri", MongoURI) \
        .enableHiveSupport() \
        .getOrCreate()
    sc = spark.sparkContext
    
    # Định nghĩa các trường của structType question
    question_schema = StructType([
        StructField("Id", IntegerType(), nullable=True),
        StructField("OwnerUserId", IntegerType(), nullable=True),
        StructField("CreationDate", DateType(), nullable=True),
        StructField("ClosedDate", DateType(), nullable=True),
        StructField("Score", IntegerType(), nullable=True),
        StructField("Title", StringType(), nullable=True),
        StructField("Body", StringType(), nullable=True)
    ])

    # Đọc collection "question" từ MongoDB
    questions_df = spark.read.format("com.mongodb.spark.sql.DefaultSource") \
        .option("uri", MongoURI) \
        .option("collection", "questions") \
        .load()
    questions_df = questions_df.withColumn("OwnerUserId", F.col("OwnerUserId").cast("integer"))
    questions_df = questions_df.withColumn("CreationDate", F.to_date("CreationDate"))
    questions_df = questions_df.withColumn("ClosedDate", F.to_date("ClosedDate"))

    questions_df_show = questions_df.show()

    # Định nghĩa các trường của structType question
    answers_schema = StructType([
        StructField("Id", IntegerType(), nullable=True),
        StructField("OwnerUserId", IntegerType(), nullable=True),
        StructField("CreationDate", DateType(), nullable=True),
        StructField("ParentId", IntegerType(), nullable=True),
        StructField("Score", IntegerType(), nullable=True),
        StructField("Body", StringType(), nullable=True)
    ])
    answers_df = spark.read.format("com.mongodb.spark.sql.DefaultSource") \
        .option("uri", MongoURI) \
        .option("collection", "answers") \
        .load()
    answers_df_show = answers_df.show()

    # Chuyển đổi dữ liệu cho trường CreationDate và ClosedDate và xóa cột Oid
    answers_df = answers_df.withColumn("OwnerUserId", F.col("OwnerUserId").cast("integer"))
    answers_df = answers_df.withColumn("ParentId", F.col("ParentId").cast("integer"))
    answers_df = answers_df.withColumn("CreationDate", F.to_date("CreationDate")) 

    # Yêu cầu 1: Tính số lần xuất hiện của các ngôn ngữ lập trình
    # Viết hàm UDF để tách các loại ngôn ngữ
    def extract_languages(string):
        lang_regex = r"Java|Python|C\+\+|C\#|Go|Ruby|Javascript|PHP|HTML|CSS|SQL"
        if string is not None:
            return re.findall(lang_regex, string)

    extract_languages_udf = F.udf(extract_languages, returnType=ArrayType(StringType()))
    languages_df = questions_df.withColumn('Body_extract', extract_languages_udf('Body')) \
        .withColumn('Programing Language', F.explode('Body_extract')) \
        .groupBy('Programing Language') \
        .agg(F.count('Programing Language').alias('Count'))

    languages_df.show()

    # Yêu cầu 2 : Tìm các domain được sử dụng nhiều nhất trong các câu hỏi
    # Viết UDF tìm Domain name
    def extract_domain(string):
        domain_regex = r"https?://([a-zA-Z0-9.-]+\.[a-zA-Z]{2,6})"
        if string is not None:
            return re.findall(domain_regex, string)

    extract_domain_udf = F.udf(extract_domain, returnType=ArrayType(StringType()))
    domain_df = questions_df.withColumn("Domain_extract", extract_domain_udf("Body")) \
        .withColumn("Domain", F.explode("Domain_extract")) \
        .groupBy("Domain") \
        .agg(F.count("Domain")) \
        .sort(F.col("count(Domain)").desc())

    domain_df.show(20)

     # Yêu cầu 3 : Tính tổng điểm của User theo từng ngày
    score_total_window = Window.partitionBy("OwnerUserId") \
        .orderBy("CreationDate") \
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    score_df = questions_df.select("OwnerUserId", "CreationDate", "Score") \
        .withColumn("Total_Score", F.sum("Score").over(score_total_window)) \
        .drop("Score")

    score_df.show(50)

    #Yêu cầu 4: Tính tổng số điểm mà User đạt được trong một khoảng thời gian
    START = '01-01-2008'
    END = '01-01-2009'
    StartDate = datetime.strptime(START, "%d-%m-%Y")
    EndDate = datetime.strptime(END, "%d-%m-%Y")
    Day_df = questions_df.select("CreationDate","OwnerUserId", "Score") \
                .where(F.col("CreationDate").between(StartDate, EndDate)) \
                .drop(F.col("CreationDate"))

    Day_Score_df = Day_df.groupBy(F.col("OwnerUserId")) \
                        .agg(F.sum(F.col("Score"))) \
                        .sort(F.col("OwnerUserId"))

    Day_Score_df.show()
    #Yêu cầu 5: Tìm các câu hỏi có nhiều câu trả lời
    # Thực hiện Join 
    """ spark.sql("CREATE DATABASE IF NOT EXISTS MY_DB")
    spark.sql("USE MY_DB")
    
    # Chia Bucket để Join 
    questions_df.coalesce(1).write \
        .bucketBy(10, "Id") \
        .format("parquet") \
        .mode("overwrite") \
        .option("path", "{}/data_questions".format(DataDir)) \
        .saveAsTable("MY_DB.questions")
    
    answers_df.coalesce(1).write \
        .bucketBy(10, "ParentId") \
        .format("parquet") \
        .mode("overwrite") \
        .option("path", "{}/data_answers".format(DataDir)) \
        .saveAsTable("MY_DB.answers") 
    spark.sql("USE MY_DB")
    questions_df = spark.read.table("MY_DB.questions")
    
    answers_df = spark.read.table("MY_DB.answers")
    
    
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
     
    questions_df = questions_df.withColumnRenamed("Id", "QuestionId")
    questions_df = questions_df.withColumnRenamed("OwnerUserId", "QuestionerID")
    questions_df = questions_df.withColumnRenamed("Score", "QuestionScore")
    questions_df = questions_df.withColumnRenamed("CreationDate", "QuestionCreationDate")
    
    # Điều kiện join
    join_expr = questions_df.QuestionId == answers_df.ParentId

    # Thực hiện join
    join_df = questions_df.join(answers_df, join_expr, "inner")
    
    # Thực hiện tính toán và lọc
    join_df_count = join_df.select("QuestionId", "Id") \
                        .groupBy("QuestionId") \
                        .agg(F.count("Id").alias("Number of answers")) \
                        .sort(F.asc("QuestionId")) \
                        .filter(F.col("Number of answers") > 5)
    
    join_df_count.show()

    # Yêu cầu 6: Tìm các Active User
    # Đọc dữ liệu từ các bảng
    questions_table = spark.read.table("MY_DB.questions")
    answers_table = spark.read.table("MY_DB.answers")


    # Lọc user có 50 câu trả lời hoặc tổng số điểm đạt được khi trả lời lớn hơn 500.
    active_df1 = answers_table.groupBy("OwnerUserId") \
        .agg(F.count("Id").alias("AnswersCount"), F.sum("Score").alias("ScoreTotal")) \
        .where((F.col("AnswersCount") > 50) | (F.col("ScoreTotal") > 500)) \
        .where(F.col("OwnerUserId").isNotNull()) \
        .sort(F.col("OwnerUserId")) \
        .select("OwnerUserId")
        

    # Tạo DataFrame chứa ngày tạo và id của câu hỏi
    questions_create_date_df = questions_table.select("CreationDate", "Id")

    # Xác định điều kiện cho join
    conditions = (questions_create_date_df.Id == answers_table.ParentId) & \
                 (questions_create_date_df.CreationDate == answers_table.CreationDate)

    # Lọc user có nhiều hơn 5 câu trả lời trong ngày câu hỏi được tạo
    active_df2 = answers_table.join(questions_create_date_df, conditions, "inner") \
        .groupBy("OwnerUserId") \
        .agg(F.count("*").alias("Answer_Count")) \
        .where((F.col("Answer_Count") > 5) & (F.col("OwnerUserId").isNotNull())) \
        .sort("OwnerUserId") \
        .select("OwnerUserId")

    active_user = active_df1.union(active_df2).distinct() \
                            .sort(F.col("OwnerUserId"))
    active_user.show() """