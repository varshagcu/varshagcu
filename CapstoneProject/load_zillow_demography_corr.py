from sqlalchemy import create_engine
import pymysql
import pandas as pd
import os
import numpy as np

#find the correlation based on demography and zip codes
def get_corr(df,county_name,year):
    return df[county_name,year]


uszips_df = pd.read_csv('\\Users\\varshadamodaran\\CapstoneProject\\AmericanCommunitySurvey\\3.uszips\\uszips.csv')

uszips_df = uszips_df[['zip','county_name']]


directory = '\\Users\\varshadamodaran\\CapstoneProject\\AmericanCommunitySurvey\\2.zillow_demography'

tableName = "zillow_demography_corr"


for filename in os.listdir(directory):

    f = os.path.join(directory, filename)
    # checking if it is a file
    if os.path.isfile(f):
        print(f)

    sqlEngine = create_engine('mysql+pymysql://root:Root#123@localhost/team30')

    zd_df = pd.read_csv(f)
    dataFrame = pd.merge(zd_df,uszips_df,how='inner', left_on='Zip', right_on='zip')
    dataFrame.drop(columns='Zip',inplace=True)
    dataFrame.dropna(inplace=True)

    corr_df = dataFrame.groupby(['county_name','zip','Concept','Variable_Label'])[['Population','Avg_value']].corr().unstack().iloc[:,1]
    print(corr_df.head())
    corr_df = corr_df.reset_index()
    corr_df.columns = ['county','zip','demography_type','demography_sub_type','corr']
    print(corr_df.head())

    dbConnection = sqlEngine.connect()
    print('DB connected')
    try:
        frame = corr_df.to_sql(tableName, dbConnection, if_exists='append',index=False)
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

    del corr_df
    del dataFrame
    del zd_df
