from pyspark.sql import SparkSession
from pyspark.sql.types import (IntegerType, FloatType, StringType, StructField,
                               StructType, TimestampType)
						   
spark = SparkSession.builder.appName("Read CSV").getOrCreate()

def read_csv():
	csv_students = StructType([StructField('student_id', IntegerType()),
                         StructField('name', StringType()),
                         StructField('career', StringType()),
                         ])

	csv_career = StructType([StructField('career_id', IntegerType()),
                         StructField('credits', IntegerType()),
                         StructField('career', StringType()),
                         ])

	csv_grade = StructType([StructField('student_id', IntegerType()),
                         StructField('career_id', IntegerType()),
                         StructField('grade', FloatType()),
                         ])

	students_dataframe = spark.read.csv("students.csv",
                           schema=csv_students,
                           header=False)
						   
	career_dataframe = spark.read.csv("career.csv",
                           schema=csv_career,
                           header=False)
						   
	grade_dataframe = spark.read.csv("grades.csv",
                           schema=csv_grade,
                           header=False)
						   
						   
	return students_dataframe, career_dataframe, grade_dataframe