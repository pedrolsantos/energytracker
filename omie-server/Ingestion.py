# pip install openpyxl
# pip install requests
# pip install pyarrow

import os
import datetime
import pandas as pd
import requests
import json
import hashlib

from logger_config import setup_logger


### CLASSES ###

class FilesHashes ():
    hash_file = None

    def __init__(self, folder, hash_file = 'hashes.json'):
        self.hash_file = os.path.join(folder, hash_file)
        pass

    def get_file_hash(self, file_path):
        with open(file_path, 'rb') as f:
            file_hash = hashlib.md5(f.read()).hexdigest()
        return file_hash

    def save_hashes(self, folder):
        hashes = {}
        for file in os.listdir(folder):
            if file.endswith('.1'):
                file_path = os.path.join(folder, file)
                hashes[file] = self.get_file_hash(file_path)
        with open(self.hash_file, 'w') as f:
            json.dump(hashes, f)

    def load_hashes(self):
        with open(self.hash_file, 'r') as f:
            return json.load(f)

    def hashfile_created(self):
        return os.path.exists(self.hash_file)

    def csvs_changed(self, folder ):
        """
        Check if there are any changed or new CSV files in the folder.

        This function calculates the hash of each CSV file in the folder with a '.1' extension
        and compares it with the previously saved hashes. If there's a mismatch, it means
        the CSV file has been changed. If there are new files with a '.1' extension, the function
        also considers them as changed files. If any changed or new CSV files are found,
        the function returns True; otherwise, it returns False.

        Args:
            folder (str): The path to the folder containing the CSV files.

        Returns:
            bool: True if there are changed or new CSV files, False otherwise.
        """        
        old_hashes = self.load_hashes()
        current_files = set(os.listdir(folder))

        for file in current_files:
            if file.endswith('.1'):
                file_path = os.path.join(folder, file)
                new_hash = self.get_file_hash(file_path)
                if old_hashes.get(file) != new_hash:
                    return True

        old_files = set(old_hashes.keys())
        new_files = current_files.difference(old_files)
        new_csv_files = any([file.endswith('.1') for file in new_files])

        return new_csv_files



class DataIngestion():
    logger = None
    hashesManager = None
    omie_folder = None
    omie_data = None
    profile_data = None
    profile_loss_data = None
    luzBoa_data = None
    omie_data_file = 'omie_data.feather'
    profiles_data_file = 'profiles_data.feather'
    loss_data_file = 'loss_data.feather'


    def __init__(self, omie_folder = '../OMIE_Data/', eredes_folder='../ERedes_profiles/', consumption_profile_data='../ERedes_profiles/E-REDES_Perfil_Consumo_2023_mod.xlsx', loss_profile_data='../ERedes_profiles/E-REDES_Perfil_Perdas_2023_mod.xlsx'):
        self.logger = setup_logger('DataIngestion')
        self.hashesManager = FilesHashes(omie_folder)
        self.omie_folder = omie_folder
        self.omie_data_file = os.path.join(self.omie_folder, self.omie_data_file)
        self.profiles_data_file = os.path.join(eredes_folder, self.profiles_data_file)
        self.loss_data_file = os.path.join(eredes_folder, self.loss_data_file)

        # Check if the CSV files have changed
        if (not self.hashesManager.hashfile_created() ) or (self.hashesManager.csvs_changed(self.omie_folder) ) or (not os.path.exists(self.omie_data_file)) :
            self.logger.info ('OMIE Data - CSV files changed. Starting file ingestion')
            self.omie_data = self.load_OMIE_data(self.omie_folder)
            self.hashesManager.save_hashes(self.omie_folder)
        else:
            self.logger.info ('OMIE Data - CSV files not changed. Loading data from Feather file')
            self.omie_data = pd.read_feather(self.omie_data_file)
            self.omie_data.set_index('Date', inplace=True)
            self.logger.info ( 'Min date = ' + str(self.omie_data.index.min()) )
            self.logger.info ( 'Max date = ' + str(self.omie_data.index.max()) )               
            self.logger.info ( 'Total Rows in dataframe = ' + str(len(self.omie_data.index))  )        

        #self.profile_data = self.load_EREDES_ConsumptionProfiles (consumption_profile_data)
        self.profile_data = self.load_EREDES_GlobalProfiles (consumption_profile_data)
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

        # Save the data to a Feather file
        all_data.reset_index().to_feather(self.omie_data_file)

        self.logger.info ( 'Total Files Ingested = ' + str(count) )        
        self.logger.info ( 'Min date = ' + str(all_data.index.min()) )
        self.logger.info ( 'Max date = ' + str(all_data.index.max()) )               
        self.logger.info ( 'Total Rows in dataframe = ' + str(len(all_data.index))  )        
        self.logger.info ('Ingestion completed')

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

    def load_ERedes_Consumption_data (self, filename, sampling=''):
        # Load the E-Redes Consumption data
        df = pd.read_excel(filename, sheet_name='Leituras', header=None)

        # Find the row containing the header "Data", "Hora", "Consumo registado, Ativa"
        header_row_index = df.loc[(df[0] == 'Data') & (df[1] == 'Hora')].index[0]

        # df = pd.read_excel(filename, sheet_name='Leituras', skiprows=7, header=0)
        df = pd.read_excel(filename, sheet_name='Leituras', header=header_row_index)

        # Define the columns to keep
        columns_to_keep = ['Data', 'Hora', 'Consumo registado'] #, Ativa']

        # Create a list of columns to drop
        columns_to_drop = [col for col in df.columns if not any(part in col for part in columns_to_keep)]

        # Drop the unwanted columns
        df = df.drop(columns=columns_to_drop)

        # Identify the existing column in df.columns that matches one of the partial names in columns_to_keep
        column_to_rename = next((col for col in df.columns if columns_to_keep[2] in col), None)

        # rename the column
        new_column_name = 'Energy'
        df = df.rename(columns={column_to_rename: new_column_name})    

        # create a new column with the concatenation of the "Date" and "Hour" columns
        df['Date'] = pd.to_datetime(df['Data'].astype(str) +' '+ df['Hora'].astype(str) , format='%Y/%m/%d %H:%M')

        # Set the new index column and drop the original Data and Hora columns
        df.set_index('Date', inplace=True)
        df.drop(columns=['Data', 'Hora'], inplace=True)
        df = df.sort_index()

        # Resample the data to hourly frequency
        df = df.resample(sampling).sum() if sampling else df

        # Check if any value in the Energy column is not valid (NaN or not >= 0)
        if not (df[new_column_name].notnull() & (df[new_column_name] >= 0)).all():
            df = None
    
        # Check for the existence of the "Contador" sheet
        try:
            contador_df = pd.read_excel(filename, sheet_name='Contador', header=None)
        except Exception as e:
            contador_df = None
        
        leituras = []
        if contador_df is not None:
            header_row_index = contador_df.loc[(contador_df[0] == 'Data da Leitura') & (contador_df[1] == 'Origem')].index[0]

            # Load the table data into the JSON structure
            readings_df = contador_df.iloc[header_row_index + 1:]
            readings_df.columns = contador_df.iloc[header_row_index]
            
            for index, row in readings_df.iterrows():
                leitura = {
                    'Data_da_Leitura': row['Data da Leitura'].strftime('%Y-%m-%d 00:00'),
                    'Vazio': row['Vazio'],
                    'Ponta': row['Ponta'],
                    'Cheias': row['Cheias']
                }
                leituras.append(leitura)

        # delete the file as is not needed anymore
        try:
            os.remove(filename)
        except Exception as error:
            self.logger.error("Error removing file: %s", error)        

        return df, leituras

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
        df[['BTN A', 'BTN B', 'BTN C', 'IP']] = df[['BTN A', 'BTN B', 'BTN C', 'IP']].apply(pd.to_numeric)

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
    
    def load_EREDES_GlobalProfiles (self, filename):
        # ERSE_perfis_de_consumo_2023_especial.xlsx
        # Load the E-Redes Consumption Profiles

        # check if feather file exists
        if ( os.path.exists (self.profiles_data_file) ):
            self.logger.info (f'Loading E-Redes Consumption Profile from Feather file')
            df = pd.read_feather(self.profiles_data_file)
            df = df.set_index('Date')
            return df

        # Load the E-Redes Consumption Profiles if the Feather file does not exist
        self.logger.info (f'Loading E-Redes Consumption Profile file= {filename}')

        df = pd.read_excel(filename, sheet_name='2023', skiprows=7, header=None)
        df.columns = ['Data', 'Dia', 'Hora', 'RESP', 'BTN-A', 'BTN-B', 'BTN-C', 'IP', 'mP', 'UPAC-A-CV-Consumo', 'UPAC-A-CV-Injecao', 'UPAC-B-CV-Consumo', 'UPAC-B-CV-Injecao', 'UPAC-C-CV-Consumo', 'UPAC-C-CV-Injecao', 'UPAC-A-Consumo', 'UPAC-B-Consumo', 'UPAC-C-Consumo']

        # Convert the columns to numeric values
        df[['RESP', 'BTN-A', 'BTN-B', 'BTN-C', 'IP', 'mP', 'UPAC-A-CV-Consumo', 'UPAC-A-CV-Injecao', 'UPAC-B-CV-Consumo', 'UPAC-B-CV-Injecao', 'UPAC-C-CV-Consumo', 'UPAC-C-CV-Injecao', 'UPAC-A-Consumo', 'UPAC-B-Consumo', 'UPAC-C-Consumo']] = df[['RESP', 'BTN-A', 'BTN-B', 'BTN-C', 'IP', 'mP', 'UPAC-A-CV-Consumo', 'UPAC-A-CV-Injecao', 'UPAC-B-CV-Consumo', 'UPAC-B-CV-Injecao', 'UPAC-C-CV-Consumo', 'UPAC-C-CV-Injecao', 'UPAC-A-Consumo', 'UPAC-B-Consumo', 'UPAC-C-Consumo']].apply(pd.to_numeric)

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
        df.drop(columns=['Data', 'Dia', 'Hora', 'RESP'], inplace=True)

        # Save the data to a Feather file
        df.reset_index().to_feather(self.profiles_data_file)

        return df

    def load_EREDES_LossesProfiles (self, filename):
        # Load the E-Redes Losses Profiles
        
        # check if feather file exists
        if ( os.path.exists (self.loss_data_file) ):
            self.logger.info (f'Loading E-Redes Losses Profile from Feather file')
            df = pd.read_feather(self.loss_data_file)
            df = df.set_index('Date')
            return df

        # Load the E-Redes Loss Profile if the Feather file does not exist
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

        # Save the data to a Feather file
        df.reset_index().to_feather(self.loss_data_file)

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
        if response.status_code != 200:
            self.logger.error (f"Failed to fetch data from API. Status code: {response.status_code}")
            return None
        
        return response.json()            
