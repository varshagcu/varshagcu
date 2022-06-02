# import required module
import os
from sqlalchemy import create_engine
import pymysql
import pandas as pd
import datetime
# assign directory
directory = '\\Users\\varshadamodaran\\CapstoneProject\\API\\zillow\\'
# directory = '/home/ec2-user/zillow'

# iterate over files in
# that directory
for filename in os.listdir(directory):

	home_type = filename.replace('.csv','')
	f = os.path.join(directory, filename)
	# checking if it is a file
	if os.path.isfile(f):
		print(f)
	else:
		continue

        #preprocess the zillow source data with required fields
	df = pd.read_csv(f)

	value_yr = range(2011,2020)
	col_list = df.columns.to_list()
	common_col_list = ['RegionID', 'RegionName', 'RegionType', 'StateName', 'State', 'City', 'Metro', 'CountyName']
	date_list = col_list[9:]
	month_list = ['Jan_value', 'Feb_value', 'Mar_value', 'Apr_value', 'May_value', 'Jun_value', 'Jul_value', 'Aug_value', 'Sep_value', 'Oct_value', 'Nov_value', 'Dec_value']
	zillow_df = pd.DataFrame()

	for yr in value_yr:
		export_list = common_col_list.copy()
		table_cols = common_col_list.copy()
		for dt in date_list:
			if dt.startswith(str(yr)):
				month_num = dt.split('-')[1]
				datetime_object = datetime.datetime.strptime(month_num, "%m")
				month_name = datetime_object.strftime("%b")
				table_cols.append(month_name+'_value')
				export_list.append(dt)
			else:
				continue
		new_df = df.copy()
		new_df = new_df[export_list]
		new_df.columns = table_cols
		new_df['Value_year'] = yr
		new_df['Home_Type'] = home_type
		new_df.dropna(inplace=True)
		new_df['Avg_value'] = new_df[month_list].values.mean(axis=1).astype(int)
		new_df.drop(columns=month_list,inplace=True)
		zillow_df = zillow_df.append(new_df)

	print(zillow_df.shape)
	zillow_df.to_csv(directory + '\\preprocessed\\' + 'zillow_preprocessed.csv',index=False)

	#       Test the sql connection
	#       tableName = "zillow_home_value_index2"
	# 	# sqlEngine = create_engine('mysql+pymysql://root:@127.0.0.1/test', pool_recycle=3600)
	# 	sqlEngine = create_engine('mysql+pymysql://root:MySQL2021@localhost/team30')
	# 	# sqlEngine = create_engine('mysql+pymysql://root:Root#123@localhost/team30')
	# 	#  create_engine('mysql+pymysql://<username>:<password>@<host>/<dbname>')
	#
	# 	dbConnection = sqlEngine.connect()
	# 	try:
	# 		frame = dataFrame.to_sql(tableName, dbConnection, if_exists='append')
	# 	except ValueError as vx:
	# 		print('ValueError')
	# 		print(vx)
	# 	except Exception as ex:
	# 		print('Exception')
	# 		print(ex)
	# 	else:
	# 		print("Table %s created successfully." % tableName)
	# 	finally:
	# 		dbConnection.close()
	#
	# 	del dataFrame
	# 	del new_df
	#
	# del df
