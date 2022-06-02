from sqlalchemy import create_engine
import pymysql
import pandas as pd

import os

#cleanup the demography data and remove extra columns
demography_dir = "\\Users\\varshadamodaran\\CapstoneProject\\AmericanCommunitySurvey\\4.demography"
# demography_dir = '/home/ec2-user/demography/'
demography_df = pd.read_csv(demography_dir+'demography.csv')


def get_column_name(df):
    try:
        print(df['demography_type'],df['demography_sub_type'])
        col_name = demography_df[(demography_df['demography_type']== df['demography_type'])
                         & (demography_df['demography_sub_type']== df['demography_sub_type'])]['demo_type_subtype'].values
        print(col_name)
        return col_name

    except:
        print('NA')
        return ['NA']


zd_directory = '\\Users\\varshadamodaran\\CapstoneProject\\AmericanCommunitySurvey\\2.zillow_demography\\'
# directory = 'D:\\analysis'
# zd_directory = '/home/ec2-user/analysis/'
tableName = "zillow_demography_denorm"
cnt = 0
err_df = pd.DataFrame()
for filename in os.listdir(zd_directory):
    f = os.path.join(zd_directory, filename)
    # checking if it is a file
    if os.path.isfile(f):
        print(f)
        cnt += 1
        # if cnt > 3:
        #     break
        zd_df = pd.read_csv(f)
        zd_df.columns = ['zip','year_','population','avg_price','demography_sub_type','demography_type']
        # print(zd_df.head())
        # print(zd_df.shape)

        new_col_name = get_column_name(zd_df.loc[0,['demography_type','demography_sub_type']])
        if new_col_name == 'NA':
            print('No data in demography........')
        elif len(new_col_name) == 0 :
            print('Data missing: ', zd_df.loc[0,['demography_type','demography_sub_type']])
            err_df.append(zd_df.loc[0,['demography_type','demography_sub_type']])
        else:
            print(new_col_name)
            new_col_name = new_col_name[0]
            zd_df.columns = ['zip','year_',new_col_name,'avg_price','demography_sub_type','demography_type']
            # print(zd_df.head())
            zd_df = zd_df[['zip','year_','avg_price',new_col_name]]

            if cnt == 1:
                df = zd_df.copy()
            else:
                df = pd.merge(df,zd_df,how="left",left_on=['zip','year_'], right_on=['zip','year_'])


# df.to_csv('zillow_demography_denorm.csv',index=False)
# err_df.to_csv('error.csv',index=False)
print(df.shape)
sqlEngine = create_engine('mysql+pymysql://root:Root#123@localhost/team30')
dbConnection = sqlEngine.connect()
print('DB connected')
try:
    frame = df.to_sql(tableName, dbConnection, if_exists='append',index=False)
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
