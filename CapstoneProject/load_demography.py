from sqlalchemy import create_engine
import pymysql
import pandas as pd
import os

#load the demography data 
directory = '\\Users\\varshadamodaran\\CapstoneProject\\AmericanCommunitySurvey\\4.demography'
# directory = '/home/ec2-user/demography'
tableName = 'demography_dim'
df = pd.DataFrame()
for filename in os.listdir(directory):
    f = os.path.join(directory, filename)
    # checking if it is a file
    if os.path.isfile(f):
        print(f)

        sqlEngine = create_engine('mysql+pymysql://root:Root#123@localhost/team30') 'mysqlcall
        with pd.read_csv(f, chunksize=500000) as reader:
            for df in reader:
                print(df.shape)

                dbConnection = sqlEngine.connect()
                print('DB connected')
                df.dropna(inplace=True)
                try:
                    frame = df.to_sql(tableName, dbConnection, if_exists='append', index=False) 'create the table and load data
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
