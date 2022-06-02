import pandas as pd
from sqlalchemy import create_engine
import pymysql
import os

'load US zip codes
directory = '\\Users\\varshadamodaran\\CapstoneProject\\AmericanCommunitySurvey\\3.uszips'
# directory = '/home/ec2-user/uszips'
filename = "uszips.csv"
tableName = "uszips"
f = os.path.join(directory, filename)


sqlEngine = create_engine('mysql+pymysql://root:Root#123@localhost/team30')

dataFrame = pd.read_csv(f)
print(dataFrame.head())
dbConnection = sqlEngine.connect()
print('DB connected')
try:
    frame = dataFrame.to_sql(tableName, dbConnection, if_exists='append', index=False) 'load uszips to table
except ValueError as vx:
    print('ValueError')
    print(vx)
except Exception as ex:
    print('Exception')
    print(ex)
else:
    print("Table %s created successfully." % tableName)
finally:
    dbConnection.close()
