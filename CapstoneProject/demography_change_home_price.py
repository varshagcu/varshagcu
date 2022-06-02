import requests
import pandas as pd
import time

#load the data from census beauro
def get_demography_data(var_name,var_label):
    # key for API
    key = 'b23793a57bc7bd8ac9459fc066cddbc727da8a5e'
    demography_df = pd.DataFrame()
    demography_dtype = {'Population':int,'zip_code_tabulation_area': int,'year':int}
    print(var_label)
    for yr in range(2011,2020): 'test for yr in range(2019,2020):
        print(yr)
        url = f"https://api.census.gov/data/{yr}/acs/acs5?get={var_name}&for=zip%20code%20tabulation%20area:*&key={key}"
        print(url)
        response = requests.request("GET", url)
        print(response)
        if response.status_code != 200:
            print('cannot find the link')
            continue
        df = pd.DataFrame(response.json()[1:], columns=response.json()[0])
        # print(df.head())
        df.dropna(inplace=True)
        df.drop(columns='state', inplace=True)
        df['year'] = yr
        df.columns = ['Population','zip_code_tabulation_area','year']
        df = df.astype(demography_dtype)
        print(df.shape)
        demography_df = demography_df.append(df)
    return demography_df

directory = '\\Users\\varshadamodaran\\CapstoneProject\\AmericanCommunitySurvey\\zillow\\preprocessed\\'
zillow_dtype = {'RegionName':int,'Value_year':int, 'Avg_value':int}
zillow_read_cols = ['RegionName','Value_year', 'Avg_value']
zillow_df = pd.read_csv(directory+'zillow_preprocessed.csv',usecols=zillow_read_cols,dtype=zillow_dtype)
zillow_df.columns = ['Zip', 'Value_year', 'Avg_value']

var_df = pd.read_excel('\\Users\\varshadamodaran\\CapstoneProject\\AmericanCommunitySurvey\\0.util\\API_variables.xlsx')
print('Rows: ', var_df.shape[0])
corr_label = []
corr_value = []
corr_concept = []
for index, row in var_df.iterrows():
    var_name = row[0]
    var_label = row[1]
    concept = row[2]
    demography_df = get_demography_data(var_name,var_label)
    demography_df['Concept'] = concept
    print(demography_df.head())
    print(demography_df.shape)

    new_df = pd.merge(zillow_df,demography_df,how='inner',left_on=['Zip', 'Value_year'],right_on=['zip_code_tabulation_area','year'])
    print(new_df.head())
    new_df['Variable_Label'] = var_label
    print(new_df.head())
    print(new_df.columns)

    new_df[['Zip', 'year','Population','Avg_value','Variable_Label','Concept']].to_csv('\\Users\\varshadamodaran\\CapstoneProject\\AmericanCommunitySurvey\\analysis\\new\\'+concept+'_'+ var_label+'.csv',index=False)





