def join_dataframes_one_column(left,right,column,method):
	return left.join(right,column,how=method)

def join_dataframes_two_columns(left,right,column1,column2,method):
	return left.join(right, [column1,column2], how=method)
	
