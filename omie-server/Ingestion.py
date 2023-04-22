# pip install openpyxl
# pip install requests
import os
import datetime
import pandas as pd
import sqlite3
import requests
import json
from logger_config import setup_logger


### CLASSES ###
class DataIngestion():
    logger = None
    omie_folder = None
    omie_data = None
    profile_data = None
    profile_loss_data = None

    def __init__(self, folder = '../OMIE_Data/', consumption_profile_data='../ERedes_profiles/E-REDES_Perfil_Consumo_2023_mod.xlsx', loss_profile_data='../ERedes_profiles/E-REDES_Perfil_Perdas_2023_mod.xlsx'):
        self.logger = setup_logger('DataIngestion')
        self.omie_folder = folder
        self.omie_data = self.load_OMIE_data(self.omie_folder)
        self.profile_data = self.load_EREDES_ConsumptionProfiles (consumption_profile_data)
        self.profile_loss_data = self.load_EREDES_LossesProfiles (loss_profile_data)

    def load_OMIE_data(self, folder ):
        # Load OMIE data from CSV files into a Pandas DataFrame
        # The CSV files are downloaded from the OMIE website

        csv_columns = ['Year', 'Month', 'Day', 'Hour', 'Price_PT', 'Price_ES']
        # Create an empty DataFrame with the same columns as the CSV files
        all_data = pd.DataFrame(columns= csv_columns)

        # Define the data types for each column
        dtypes = {'Year': int, 'Month': int, 'Day': int,  'Price_PT': float, 'Price_ES': float}

        self.logger.info ('OMIE Data - Start file Ingestion on folder: ' + folder)

        # Iterate over the CSV files in a directory
        count = 1
        for csv_file in os.listdir(folder):
            if csv_file.endswith('.1'):
                filename = os.path.join(folder, csv_file)
                # Read the CSV file into a temporary DataFrame
                # Convert the Hour column to integer and subtract 1 hour to get the correct hour in PT

                df = pd.read_csv(filename, delimiter=';', names= csv_columns, skiprows=[0], skipfooter=1, engine='python', index_col=False, dtype=dtypes, converters={'Hour': lambda x: int(x)-1} )
                # print ( f'\t{count} - File ´{filename}´ processed with {str(len(df.index)) } rows')

                # Append the temporary DataFrame to the main DataFrame
                all_data = pd.concat([all_data, df])
                count+=1

        # Define new index column as concatenation of Year, Month, Day, and Hour columns
        all_data['Date'] = pd.to_datetime(all_data['Year'].astype(str) +'-'+ all_data['Month'].astype(str) +'-'+ all_data['Day'].astype(str) +'-'+ all_data['Hour'].astype(str)+':00', format='%Y-%m-%d-%H:%M')

        # Set the new index column and drop the original Year, Month, Day, and Hour columns
        all_data.set_index('Date', inplace=True)
        all_data.drop(columns=['Year', 'Month', 'Day', 'Hour'], inplace=True)
        all_data = all_data.sort_index()

        self.logger.info ( 'Total Files Ingested = ' + str(count) )        
        self.logger.info ( 'Min date = ' + str(all_data.index.min()) )
        self.logger.info ( 'Max date = ' + str(all_data.index.max()) )               
        self.logger.info ( 'Total Rows in dataframe = ' + str(len(all_data.index))  )        
        self.logger.info ('Ingestion completed')

        # NOTE: Uncomment the following lines to save the data to a SQLite database
        # Connect to SQLite database
        #conn = sqlite3.connect('omie_data.db')

        # Insert data into SQLite database
        #all_data.to_sql('Prices', conn, if_exists='replace', index=True, index_label='Date')

        # Close database connection
        #conn.close()

        self.omie_data = all_data
        return all_data

    def download_OMIE_data(self, date = datetime.datetime.now()):
        # Download today's OMIE data
        # https://www.omie.es/pt/file-download?parents%5B0%5D=marginalpdbcpt&filename=marginalpdbcpt_20230324.1
        
        filename = 'marginalpdbcpt_' + date.strftime('%Y%m%d') + '.1'
        url = 'https://www.omie.es/pt/file-download?parents%5B0%5D=marginalpdbcpt&filename=' + filename

        response = requests.get(url)

        # Ensure the request was successful
        saved = False
        size = len(response.content)

        if (response.status_code == 200) and (size > 0):
            # Choose the destination folder and the file name
            file_path = os.path.join(self.omie_folder, filename)

            # Save the file to the specific folder
            with open(file_path, "wb") as f:
                f.write(response.content)
                saved = True

        saved = False if size == 0 else saved
        status = {'status_code': response.status_code, 'filename': filename, 'size': size, 'saved': saved}
        return status

    def load_ERedes_Consumption_data (self, filename):
        # Load the E-Redes Consumption data
        df = pd.read_excel(filename, sheet_name='Leituras', header=None)

        # Find the row containing the header "Data", "Hora", "Consumo registado, Ativa"
        header_row_index = df.loc[(df[0] == 'Data') & (df[1] == 'Hora')].index[0]

        # df = pd.read_excel(filename, sheet_name='Leituras', skiprows=7, header=0)
        df = pd.read_excel(filename, sheet_name='Leituras', header=header_row_index)

        # Define the columns to keep
        columns_to_keep = ['Data', 'Hora', 'Consumo registado, Ativa']

        # Create a list of columns to drop
        columns_to_drop = [col for col in df.columns if col not in columns_to_keep]

        # Drop the unwanted columns
        df = df.drop(columns=columns_to_drop)

        # rename the column
        df = df.rename(columns={'Consumo registado, Ativa': 'Energy'})    

        # create a new column with the concatenation of the "Date" and "Hour" columns
        df['Date'] = pd.to_datetime(df['Data'].astype(str) +' '+ df['Hora'].astype(str) , format='%Y/%m/%d %H:%M')

        # Set the new index column and drop the original Data and Hora columns
        df.set_index('Date', inplace=True)
        df.drop(columns=['Data', 'Hora'], inplace=True)
        df = df.sort_index()

        return df

    def load_EOT_Consumption_data (self, filename):
        # Load the EOT Consumption data
        # From myEot.pt

        df = pd.read_excel(filename, sheet_name='Sheet1', skiprows=0, header=0)
        
        # rename the column
        df = df.rename(columns={'Energia': 'Energy'})
        df = df.rename(columns={'Dia': 'Date'})
        
        # Converting 'Date' column to datetime dtype
        df['Date'] = pd.to_datetime(df['Date'])

        # Removing timezone information
        df['Date'] = df['Date'].dt.tz_localize(None)

        # Set the new index column and drop the original Data and Hora columns
        df.set_index('Date', inplace=True)
        df = df.sort_index()

        return df
    
    def load_EREDES_ConsumptionProfiles (self, filename):
        # Load the E-Redes Consumption Profiles

        self.logger.info (f'Loading E-Redes Consumption Profile file= {filename}')
        df = pd.read_excel(filename, sheet_name='Perfis Consumo', skiprows=4, header=None)
        df.columns = ['Data', 'Dia', 'Hora', 'BTN A', 'BTN B', 'BTN C', 'IP']

        # Convert the columns to numeric values
        df[['BTN A', 'BTN B', 'BTN C']] = df[['BTN A', 'BTN B', 'BTN C']].apply(pd.to_numeric)

        # Convert the 'Hora' column to the 'hh:mm:ss' format
        df['Hora'] = pd.to_timedelta( df['Hora'].apply(lambda x: x + ':00' if len(x) == 5 else '00:00:00' if x== '24:00' else x) )

        # Subtract 15 minutes from each value in the 'Hora' column
        df['Hora'] = df['Hora'] - pd.to_timedelta('00:15:00')

        # Create the "Date" column by adding "Data" and "Hora"
        df['Date'] = df['Data'] + df['Hora']

        # Set the "Date" column as the index
        df.set_index('Date', inplace=True)
        df = df.sort_index()

        # Drop the unnecessary columns
        df.drop(columns=['Data', 'Dia', 'Hora' ], inplace=True)

        return df
    
    def load_EREDES_LossesProfiles (self, filename):
        # Load the E-Redes Losses Profiles
        
        self.logger.info (f'Loading E-Redes Losses Profile file= {filename}')
        df = pd.read_excel(filename, sheet_name='Perfis Perdas', skiprows=4, header=None)
        df.columns = ['Data', 'Dia', 'Hora', 'BT', 'MT', 'AT', 'ATRNT', 'MAT']

        # Convert the columns to numeric values
        df[['BT', 'MT', 'AT', 'ATRNT', 'MAT']] = df[['BT', 'MT', 'AT', 'ATRNT', 'MAT']].apply(pd.to_numeric)

        # Convert the 'Hora' column to the 'hh:mm:ss' format
        df['Hora'] = pd.to_timedelta( df['Hora'].apply(lambda x: x + ':00' if len(x) == 5 else '00:00:00' if x== '24:00' else x) )

        # Subtract 15 minutes from each value in the 'Hora' column
        df['Hora'] = df['Hora'] - pd.to_timedelta('00:15:00')

        # Create the "Date" column by adding "Data" and "Hora"
        df['Date'] = df['Data'] + df['Hora']

        # Set the "Date" column as the index
        df.set_index('Date', inplace=True)
        df = df.sort_index()

        # Drop the unnecessary columns
        df.drop(columns=['Data', 'Dia', 'Hora' ], inplace=True)

        return df

    def get_EOT_Data(self, api_key, results=1, channel='iap_diff'):
        # Fetch data from EOT API
        self.logger.info (f"get_EOT_Data: Fetching data from EOT API. Results: {results}, Channel: {channel}")

        url = "https://api.eot.pt/api/meter/feed.json"
        params = {
            "key": api_key,
            "results": results,
            "channel": channel
        }
        response = requests.get(url, params=params)
        if response.status_code == 200:
            self.logger.error (f"Failed to fetch data from API. Status code: {response.status_code}")
            return None
        
        return response.json()            
