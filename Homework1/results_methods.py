# -*- coding: utf-8 -*-
"""
Created on Thu Sep  3 11:09:01 2020

@author: tavov
"""
import pyspark.sql.functions as psf
from pyspark.sql.window import Window
from pyspark.sql.functions import rank,col
from join_methods import join_dataframes_one_column


def get_max_career_values(df,student_id,career_id,name,career,grade,credit):
    result = df.groupBy(student_id,career_id).agg(psf.max(name).alias(name),psf.max(career).alias(career),psf.max(grade).alias(grade),psf.max(credit).alias(credit))   
    return result

def remove_null_values(df,column):
    result = df.filter(column. isNotNull())
    return result

def get_segregated_tables(df,column):
    values_list = df.select(column).distinct().rdd.flatMap(lambda x: x).collect()
    result = [df.where(df[column]==value) for value in values_list]

    return result

def get_weighted_result_per_student(df,credit):
    for i in range(len(df)):
        df[i] = df[i].withColumn('total_credits',sum(df[i].credit))
        df[i] = df[i].withColumn('weighted_grade',sum(df[i].credit*df[i].grade)/df.total_credits)
    return df
 
def get_weigthed_grade(df,student_id,credit,grade,sum_credits,weighted_grade):
    for dataf in df:
        dataf_credits = dataf.groupBy(student_id).sum(credit)
        dataf = join_dataframes_one_column(dataf,dataf_credits,student_id,'left')
        dataf = dataf.withColumn(weighted_grade,(dataf[credit]*dataf[grade])/(dataf[sum_credits]))
    return df

def get_top_students(df,partition_column,rank_column,level):
    window = Window.partitionBy(partition_column).orderBy(col(rank_column).desc())
    df = df.withColumn('rank',rank().over(window)).alias('rank')    
    df = df.filter(col('rank')<=level)
    return df
 
