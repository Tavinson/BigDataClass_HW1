from .joiner import join_dataframes

#One student can attend multiple careers
def test_join_one_student_multiple_careers():
    student_data = [(1, 'John Snow','Mercadeo'), (2, 'Arya Stark','Esgrima'),(5,'Tyrion Lannister','Filosofía'),(5,'Tyrion Lannister','Mercadeo')]
    student_ds = spark_session.createDataFrame(student_data,['student_id', 'name','career'])

	career_data=[(1, 4,'Mercadeo'), (2, 3,'Esgrima'),(5,1,'Filosofía')]
    career_ds = spark_session.createDataFrame(career_data,['career_id', 'credits','career_name'])

    actual_ds = join_dataframes(student_ds, career_ds, ['career'], ['career_name'])

    expected_ds = spark_session.createDataFrame(
        [
            (1, 'John Snow', 'Mercadeo', 1,4,'Mercadeo'),
            (2, 'Arya Stark', 'Esgrima', 2,3,'Esgrima'),
            (5, 'Tyrion Lannister', 'Filosofía', 5,1,'Filosofía'),
            (5, 'Tyrion Lannister', 'Mercadeo', 1,4,'Mercadeo'),
        ],
        ['student_id', 'name', 'career', 'career_id','credits','career_name'])

    expected_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == expected_ds.collect()

#Students with no matching or empty careers should be ruled out
def test_join_one_student_no_matching_careers():
    student_data = [(6, 'Khal Drogo','Lenguajes'), (7, 'Khaleesi',''),(4,'Brandon Stark','Cuervología')]
    student_ds = spark_session.createDataFrame(student_data,['student_id', 'name','career'])

	career_data=[(6, 3,'Lenguajes'), (2, 3,'Esgrima'),(5,1,'Filosofía')]
    career_ds = spark_session.createDataFrame(career_data,['career_id', 'credits','career_name'])

    actual_ds = join_dataframes(student_ds, career_ds, ['career'], ['career_name'])

    expected_ds = spark_session.createDataFrame(
        [
            (6, 'Khal Drogo', 'Lenguajes', 6,3,'Lenguajes'),
        ],
        ['student_id', 'name', 'career', 'career_id','credits','career_name'])

    expected_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == expected_ds.collect()
	
	
#Students with or without grades should be reported
def test_join_one_student_no_matching_careers():
    student_data = [(6, 'Khal Drogo','Lenguajes'), (7, 'Khaleesi',''),(4,'Brandon Stark','Cuervología')]
    student_ds = spark_session.createDataFrame(student_data,['student_id', 'name','career'])

	career_data=[(6, 3,'Lenguajes'), (2, 3,'Esgrima'),(5,1,'Filosofía')]
    career_ds = spark_session.createDataFrame(career_data,['career_id', 'credits','career_name'])

    actual_ds = join_dataframes(student_ds, career_ds, ['career'], ['career_name'])

    expected_ds = spark_session.createDataFrame(
        [
            (6, 'Khal Drogo', 'Lenguajes', 6,3,'Lenguajes'),
        ],
        ['student_id', 'name', 'career', 'career_id','credits','career_name'])

    expected_ds.show()
    actual_ds.show()

    assert actual_ds.collect() == expected_ds.collect()
