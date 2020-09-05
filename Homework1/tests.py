from joiner import *
from results_per_student import *

#One student can attend multiple careers
def test_join_one_student_multiple_careers(spark_session):
	student_data = [(1, 'John Snow','Mercadeo'), (2, 'Arya Stark','Esgrima'),(5,'Tyrion Lannister','Filosofía'),(5,'Tyrion Lannister','Mercadeo')]
	student_ds = spark_session.createDataFrame(student_data,['student_id', 'name','career'])

	career_data=[(1, 4,'Mercadeo'), (2, 3,'Esgrima'),(5,1,'Filosofía')]
	career_ds = spark_session.createDataFrame(career_data,['career_id', 'credits','career'])

	actual_ds = join_dataframes_one_column(student_ds, career_ds,'career')

	expected_ds = spark_session.createDataFrame(
		[
			('Mercadeo',1,'John Snow',4),
			('Esgrima',2,'Arya Stark',3),
			('Filosofía',5,'Tyrion Lannister',1),
			('Mercadeo',1,'Tyrion Lannister',4),
		],
		['career','student_id', 'name', 'career_id','credits'])

	actual_ds = actual_ds.sort('student_id','career_id')
	expected_ds = expected_ds.sort('student_id','career_id')

	assert actual_ds.collect() == expected_ds.collect()

#Students with no matching or empty careers should be ruled out
def test_join_one_student_no_matching_careers(spark_session):
	student_data = [(6, 'Khal Drogo','Lenguajes'),(7, 'Khaleesi',''),(4,'Brandon Stark','Cuervología')]
	student_ds = spark_session.createDataFrame(student_data,['student_id','name','career'])

	career_data=[(6, 3,'Lenguajes'),(2, 3,'Esgrima'),(5,1,'Filosofía')]
	career_ds = spark_session.createDataFrame(career_data,['career_id','credits','career'])

	actual_ds = join_dataframes_one_column(student_ds, career_ds,'career')

	expected_ds = spark_session.createDataFrame(
		[
			('Lenguajes',6,'Khal Drogo',6,3),
			('Esgrima',None,None,2,3),
			('Filosofía',None,None,5,1),
		],
		['career','student_id', 'name','career_id','credits'])

	actual_ds = actual_ds.sort('student_id','career_id')
	expected_ds = expected_ds.sort('student_id','career_id')

	assert actual_ds.collect() == expected_ds.collect()
	
#Careers with no students should be reported
def test_join_no_students_no_matching_career(spark_session):
	student_data = [(2, 'Arya Stark','Esgrima'), (3, 'Geoffrey Baratheon','Diputado'),(4,'Brandon Stark','Filología')]
	student_ds = spark_session.createDataFrame(student_data,['student_id', 'name','career'])
	career_data=[(2, 3,'Esgrima'), (3, 6,'Diputado'),(4,2,'Filología'),(7,2,'Matemática')]
	career_ds = spark_session.createDataFrame(career_data,['career_id', 'credits','career'])

	actual_ds = join_dataframes_one_column(student_ds, career_ds, 'career')

	expected_ds = spark_session.createDataFrame(
		[
			('Esgrima',2, 'Arya Stark',2,3),
			('Diputado',3, 'Geoffrey Baratheon',3, 6),
			('Filología',4,'Brandon Stark',4,2),
			('Matemática',None,None,7,2),
			
		],
		['career','student_id', 'name','career_id','credits'])

	actual_ds = actual_ds.sort('student_id','career_id')
	expected_ds = expected_ds.sort('student_id','career_id')

	assert actual_ds.collect() == expected_ds.collect()

	
#Students with careers should be reported, regardless if they have grades or not
def test_join_one_student_with_matching_careers_with_grades_and_no_or_empty_grades(spark_session):
	student_career_data = [(6, 'Khal Drogo', 'Lenguajes', 6,3),(6, 'Khal Drogo', 'Esgrima', 2,3), (5, 'Tyrion Lannister','Mercadeo',1,4),(5,'Tyrion Lannister','Filosofía',5,1)]
	student_career_ds = spark_session.createDataFrame(student_career_data,['student_id', 'name', 'career', 'career_id','credits'])

	grades_data = [(6, 6,85.00), (None,None,None),(5,5,92.40),(6,2,None)]
	grades_ds = spark_session.createDataFrame(grades_data,['student_id', 'career_id','grade'])

	actual_ds = join_dataframes_two_columns(student_career_ds, grades_ds,'student_id','career_id')

	expected_ds = spark_session.createDataFrame(
		[
			(6,6,'Khal Drogo', 'Lenguajes',3,'Lenguajes',85.00),
			(5,1,'Tyrion Lannister','Mercadeo',4,'Mercadeo',None),
			(5,5,'Tyrion Lannister','Filosofía',1,'Filosofía',92.40),
			(6,2,'Khal Drogo','Esgrima',3,'Esgrima',None),
			
		],
		['student_id', 'career_id','name', 'career','credits','career','grade'])

	actual_ds = actual_ds.sort('student_id','career_id')
	expected_ds = expected_ds.sort('student_id','career_id')

	assert actual_ds.collect() == expected_ds.collect()

#Students can re-take multiple times the same course		
def test_join_one_student_repeated_career_multiple_grades(spark_session):
	student_career_data = [(6, 'Khal Drogo', 'Lenguajes', 6,3), (1, 'John Snow','Mercadeo',1,4),(2,'Arya Stark','Esgrima',2,3)]
	student_career_ds = spark_session.createDataFrame(student_career_data,['student_id', 'name', 'career', 'career_id','credits'])

	grades_data = [(6, 6,85.00), (1,1,78.00,),(1,1,58.20),(2,2,88.00)]
	grades_ds = spark_session.createDataFrame(grades_data,['student_id', 'career_id','grade'])

	actual_ds = join_dataframes_two_columns(student_career_ds, grades_ds, 'student_id','career_id')

	expected_ds = spark_session.createDataFrame(
		[
			(6,6,'Khal Drogo', 'Lenguajes',3,85.00),
			(1,1, 'John Snow','Mercadeo',4,78.00),
			(1,1, 'John Snow','Mercadeo',4,58.20),
			(2,2, 'Arya Stark','Esgrima',3,88.00),			
		],
		['student_id','career_id','name', 'career','credits','grade'])

	
	actual_ds = actual_ds.sort('student_id','career_id')
	expected_ds = expected_ds.sort('student_id','career_id')

	assert actual_ds.collect() == expected_ds.collect()

def test_students_take_same_career_multiple_times_best_value_is_selected(spark_session):
    joint_students_carreer_grade = [(1,'John Snow','Mercadeo',1,4,78.00),(1,'John Snow','Mercadeo',1,4,58.20),(5,'Tyrion Lannister','Filosofía',5,1,92.40),(5,'Tyrion Lannister','Filosofía',5,1,None),(2,'Arya Stark','Esgrima',2,3,88.0)]
    joint_students_carreer_grade_ds = spark_session.createDataFrame(joint_students_carreer_grade,['student_id', 'name', 'career', 'career_id','credits','grade'])

    actual_ds  = get_max_career_values(joint_students_carreer_grade_ds,'student_id','career_id','name','career','grade')
    
    expected_ds = spark_session.createDataFrame(
		[
			(1,1,'John Snow', 'Mercadeo',4,78.00),
			(5,5, 'Tyrion Lannister','Filosofía',1,92.40),
			(2,2, 'Arya Stark','Esgrima',3,88.00),			
		],
		['student_id','career_id','name', 'career','credits','grade'])

	
    actual_ds = actual_ds.sort('student_id','career_id')
    expected_ds = expected_ds.sort('student_id','career_id')

    assert actual_ds.collect() == expected_ds.collect()

def test_students_multiple_careers_best_value_is_selected(spark_session):
    joint_students_carreer_grade = [(1,'John Snow','Mercadeo',1,4,78.00),(1,'John Snow','Mercadeo',1,4,58.20),(1,'John Snow','Administración',9,5,70.00)]
    joint_students_carreer_grade_ds = spark_session.createDataFrame(joint_students_carreer_grade,['student_id', 'name', 'career', 'career_id','credits','grade'])

    actual_ds  = get_max_career_values(joint_students_carreer_grade_ds,'student_id','career_id','name','career','grade')

    expected_ds = spark_session.createDataFrame(
		[
			(1,1,'John Snow', 'Mercadeo',4,78.00),
			(1,9, 'John Snow','Administración',5,70),
		],
		['student_id','career_id','name', 'career','credits','grade'])

	
    actual_ds = actual_ds.sort('student_id','career_id')
    expected_ds = expected_ds.sort('student_id','career_id')

    assert actual_ds.collect() == expected_ds.collect()
    
def test_students_table_careers_without_students_are_filtered(spark_session):
    joint_students_data =[(5,'Tyrion Lannister','Filosofía',5,2,92.40),(None,None,'Matemática',7,2,None),(2,'Arya Stark','Esgrima',2,3,88.0)]
    joint_students_ds = spark_session.createDataFrame(joint_students_data,['student_id', 'name', 'career', 'career_id','credits','grade'])
    
    actual_ds = remove_null_values(joint_students_ds,'student_id')
    
    expected_ds = spark_session.createDataFrame(
		[
			(5,5,'Tyrion Lannister', 'Filosofía',2,92.40),
			(2,2,'Arya Stark','Esgrima',3,88.0),
		],
		['student_id','career_id','name', 'career','credits','grade'])

	
    actual_ds = actual_ds.sort('student_id','career_id')
    expected_ds = expected_ds.sort('student_id','career_id')

    assert actual_ds.collect() == expected_ds.collect()
    
def test_students_table_students_without_grades_are_filtered(spark_session):
    joint_students_data =[(4,'Brandon Stark','Filologia',4,2,78.60),(5,'Tyrion Lannister','Mercadeo',1,4,None),(5,'Tyrion Lannister','Filosofia',5,1,92.4)]
    joint_students_ds = spark_session.createDataFrame(joint_students_data,['student_id', 'name', 'career', 'career_id','credits','grade'])
    
    actual_ds = remove_null_values(joint_students_ds,'grade')
    
    expected_ds = spark_session.createDataFrame(
		[
			(4,4,'Brandon Stark','Filologia',2,78.60),
			(5,5,'Tyrion Lannister','Filosofia',1,92.4),
		],
		['student_id','career_id','name', 'career','credits','grade'])

	
    actual_ds = actual_ds.sort('student_id','career_id')
    expected_ds = expected_ds.sort('student_id','career_id')

    assert actual_ds.collect() == expected_ds.collect()
    

def test_tables_segregation_per_student(spark_session):
    joint_students_data =[(5,'Tyrion Lannister','Filosofía',5,2,92.40),(5,'Tyrion Lannister','Mercadeo',1,4,None),(2,'Arya Stark','Esgrima',2,3,88.0)]
    joint_students_ds = spark_session.createDataFrame(joint_students_data,['student_id', 'name', 'career', 'career_id','credits','grade'])
    
    actual_ds = get_segregated_tables(joint_students_ds,'student_id')
    
    expected_ds1 = spark_session.createDataFrame(
		[
			(5,5,'Tyrion Lannister', 'Filosofía',2,92.40),
			(5,1,'Tyrion Lannister','Mercadeo',4,None),
		],
		['student_id','career_id','name', 'career','credits','grade'])

    expected_ds2 = spark_session.createDataFrame(
		[
			(2,'Arya Stark','Esgrima',2,3,88.0),
		],
		['student_id','career_id','name', 'career','credits','grade'])

	
    actual_ds[0] = actual_ds.sort('student_id','career_id')
    actual_ds[1] = actual_ds.sort('student_id','career_id')
    expected_ds[0] = expected_ds.sort('student_id','career_id')
    expected_ds[1] = expected_ds.sort('student_id','career_id')

    assert (actual_ds[0].collect() == expected_ds[1].collect() and actual_ds[1].collect() == expected_ds[1].collect())

def test_tables_segregation_per_career(spark_session):
    joint_students_data =[(1,'John Snow','Mercadeo',1,4,58.20),(5,'Tyrion Lannister','Mercadeo',1,4,None),(1,'John Snow','Administración',9,5,70.0)]
    joint_students_ds = spark_session.createDataFrame(joint_students_data,['student_id', 'name', 'career', 'career_id','credits','grade'])
    
    actual_ds = get_segregated_tables(joint_students_ds,'career_id')
    
    expected_ds1 = spark_session.createDataFrame(
		[
			(1,'John Snow','Mercadeo',1,4,58.20),
			(5,'Tyrion Lannister','Mercadeo',1,4,None),
		],
		['student_id','career_id','name', 'career','credits','grade'])

    expected_ds2 = spark_session.createDataFrame(
		[
			(1,'John Snow','Administración',9,5,70.0),
		],
		['student_id','career_id','name', 'career','credits','grade'])

	
    actual_ds[0] = actual_ds.sort('student_id','career_id')
    actual_ds[1] = actual_ds.sort('student_id','career_id')
    expected_ds[0] = expected_ds.sort('student_id','career_id')
    expected_ds[1] = expected_ds.sort('student_id','career_id')

    assert (actual_ds[0].collect() == expected_ds[0].collect() and actual_ds[1].collect() == expected_ds[1].collect())
    
def test_weighted_grade_null_and_Non_Null_values_are_considered(spark_session):
    joint_students_data =[(6,'Khal Drogo','Esgrima',2,3,None),(6,'Khal Drogo','Lenguajes',6,3,85.0)]
    joint_students_ds = spark_session.createDataFrame(joint_students_data,['student_id', 'name', 'career', 'career_id','credits','grade'])
     
    actual_ds = get_weigthed_grade(joint_students_ds,'student_id','credits','grade','sum(credits)','weighted_grade')

    expected_ds = spark_session.createDataFrame(
		[
			(6,2,'Khal Drogo','Esgrima',None,3,6,None),
			(6,6,'Khal Drogo','Lenguajes',85.0,3,6,42.5),
		],
		['student_id','career_id','name', 'career','grade','credits','sum(credits)','weighted_grade'])

    actual_ds = actual_ds.sort('student_id','career_id')
    expected_ds = expected_ds.sort('student_id','career_id')

    assert actual_ds.collect() == expected_ds.collect()


def test_weighted_grade_null_values_are_considered(spark_session):
    joint_students_data =[(5,'Tyrion Lannister','Mercadeo',2,4,None),(5,'Tyrion Lannister','Filosofía',5,1,None)]
    joint_students_ds = spark_session.createDataFrame(joint_students_data,['student_id', 'name', 'career', 'career_id','credits','grade'])
     
    actual_ds = get_weigthed_grade(joint_students_ds,'student_id','credits','grade','sum(credits)','weighted_grade')

    expected_ds = spark_session.createDataFrame(
		[
			(5,2,'Tyrion Lannister','Mercadeo',None,4,5,None),
			(5,5,'Tyrion Lannister','Filosofía',None,1,5,None),
		],
		['student_id','career_id','name', 'career','grade','credits','sum(credits)','weighted_grade'])

    actual_ds = actual_ds.sort('student_id','career_id')
    expected_ds = expected_ds.sort('student_id','career_id')

    assert actual_ds.collect() == expected_ds.collect()
    
#Requires an additional Dataset
def test_weighted_grade_non_null_values_are_considered(spark_session):
    joint_students_data =[(1,'John Snow','Mercadeo',1,4,78.0),(1,'John Snow','Administración',9,5,70.0)]
    joint_students_ds = spark_session.createDataFrame(joint_students_data,['student_id', 'name', 'career', 'career_id','credits','grade'])
     
    actual_ds = get_weigthed_grade(joint_students_ds,'student_id','credits','grade','sum(credits)','weighted_grade')

    expected_ds = spark_session.createDataFrame(
		[
			(1,1,'John Snow','Mercadeo',78.0,4,9,88.0),
			(1,9,'John Snow','Administración',70.0,5,9,70.0),
            
		],
		['student_id','career_id','name', 'career','grade','credits','sum(credits)','weighted_grade'])

    actual_ds = actual_ds.sort('student_id','career_id')
    expected_ds = expected_ds.sort('student_id','career_id')

    assert actual_ds.collect() == expected_ds.collect()
    
def test_top_rank_Null_and_Non_Null_Values():
    joint_students_data =[(1,'John Snow','Mercadeo',1,4,78.0),(5,'Tyrion Lannister','Mercadeo',1,4,None),(4,'Brandon Stark','Mercadeo',1,4,76.4)]
    joint_students_ds = spark_session.createDataFrame(joint_students_data,['student_id', 'name', 'career', 'career_id','credits','grade'])

    actual_ds = get_top_students(joint_students_ds,'grade',2)

    expected_ds = spark_session.createDataFrame(
		[
			(1,1,'John Snow','Mercadeo',78.0,4,1),
			(4,1,'Brandon Stark','Mercadeo',76.4,4,2),
            
		],
		['student_id','career_id','name', 'career','grade','credits','rank'])

    actual_ds = actual_ds.sort('student_id','career_id')
    expected_ds = expected_ds.sort('student_id','career_id')

    assert actual_ds.collect() == expected_ds.collect()

def test_top_rank_Non_Null_Values():
    joint_students_data =[(1,'John Snow','Mercadeo',1,4,78.0),(3,'Geoffrey Baratheon','Mercadeo',1,4,60),(4,'Brandon Stark','Mercadeo',4,76.4)]
    joint_students_ds = spark_session.createDataFrame(joint_students_data,['student_id', 'name', 'career', 'career_id','credits','grade'])

    actual_ds = get_top_students(joint_students_ds,'career','grade',2)
    
    expected_ds = spark_session.createDataFrame(
		[
			(1,1,'John Snow','Mercadeo',78.0,4,1),
			(4,1,'Brandon Stark','Mercadeo',76.4,4,2),
            
		],
		['student_id','career_id','name', 'career','grade','credits','rank'])

    actual_ds = actual_ds.sort('student_id','career_id')
    expected_ds = expected_ds.sort('student_id','career_id')

    assert actual_ds.collect() == expected_ds.collect()
   