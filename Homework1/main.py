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

	
	joint_students_careers = join_dataframes_one_column(students_dataframe,career_dataframe,students_dataframe.career,career_dataframe.career_name)
	joint_students_careers_grades = join_dataframes_two_columns(joint_students_careers,grade_dataframe,joint_students_careers.student_id,grade.student_id,joint_students_careers.career_id,grade.career_id)


def join_dataframes_one_column(left,right, column_left, column_right):
	joint_df = left.join(right, column_left == column_right,how='inner')
	return joint_df

def join_dataframes_two_columns(left,right, column_left1, column_right1, column_left2,column_right2):
	joint_df = left.join(right, (column_left1 == column_right1) & (column_left2== column_right2), how='left')
	return joint_df


	
if __name__ == "__main__":
    main()
