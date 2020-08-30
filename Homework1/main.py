from pyspark.sql import SparkSession
#from pyspark.sql.types import (IntegerType, FloatType, StructField,
#                               StructType, TimestampType)
							   
import read_csv


def main():
	spark = SparkSession.builder.appName("Read Transactions").getOrCreate()

	students_dataframe, career_dataframe, grade_dataframe = read_csv()

	students_dataframe.show()
	career_dataframe.show()
	grade_dataframe.show()

	joint_df = join_dataframes(students_dataframe,career_dataframe,grade_dataframe)


def join_dataframes(students,career,grade):
	joint_df = students.join(career, students.career == career.career_name)
	joint_df = joint_df.join(grade, (joint_df.career_id == grade.career_id) & (joint_df.student_id== grade.student_id))
	return joint_df



	
if __name__ == "__main__":
    main()
