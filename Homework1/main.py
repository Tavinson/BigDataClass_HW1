from pyspark.sql import SparkSession
from pyspark.sql.functions import rank,col
							   
import read_csv
from join_methods import *
from results_methods import *


def main():
    spark = SparkSession.builder.appName("Read Main").getOrCreate()
    spark.sparkContext.setLogLevel('FATAL')

    students_dataframe, career_dataframe, grade_dataframe = read_csv.read_csv()

#Join Data Frames
    joint_students_careers = join_dataframes_one_column(students_dataframe,career_dataframe,'career','right')
    joint_students_careers_grades = join_dataframes_two_columns(joint_students_careers,grade_dataframe,'student_id','career_id','left')

    best_results_per_student = get_max_career_values(joint_students_careers_grades,'student_id','career_id','name','career','grade','credits')    
    best_results_no_null = remove_null_values(best_results_per_student,best_results_per_student.student_id)
    best_results_no_null.show()

    #Get Weighted Result per students
    students_dfs = get_segregated_tables(best_results_no_null,'student_id')
    students_dfs = get_weigthed_grade(students_dfs,'student_id','credits','grade','sum(credits)','weighted_grade')

    print('Students weighted grades')
    for student in students_dfs:
        student.show()
    
    #Get Top students per career
    career_tables = remove_null_values(best_results_no_null,best_results_no_null.grade)
    career_tables = get_top_students(career_tables,'career','grade',2)
    career_tables.show()

    top_students_career = get_segregated_tables(career_tables,'career_id')

    print('Best Students per Career')
    for career in top_students_career:
        career.show()        

if __name__ == "__main__":
    main()