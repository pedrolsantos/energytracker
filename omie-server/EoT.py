import requests
import pandas as pd
import json
import threading
import datetime
import pytz 
import numpy as np
from datetime import time

from Calculations import EnergyCosts, Energy_Time_Cycle, ENERGY_SUPPLIERS_PARAMETERS, ENERGY_PROFILES
from logger_config import setup_logger, setBasePath

class EoTData:
    def __init__(self, base_folder, config_file, eot_datafile):
        setBasePath(base_folder)
        self.logger = setup_logger('EoT')
        self.lastEoTdate_power = None
        self.lastEoTpower = 0
        self.config_file = config_file
        self.eot_datafile = eot_datafile
        self.config_dict = self.Read_Config()
        self.EoT_masterdata = self.Read_Data()

    def start (self, scheduled_refresh_time, master_file):
        self.scheduled_refresh_time = scheduled_refresh_time
        self.master_file = master_file
        # First run of GetEoTData
        self.GetEoTData()

    def stop (self):
        self.timer.cancel()

    def Read_Data(self):
        try:
            df = pd.read_feather(self.eot_datafile)
            df.set_index('Date', inplace=True)
            df.sort_index(inplace=True)
            df.index = pd.to_datetime(df.index)
        except FileNotFoundError:
            df = pd.DataFrame(columns=['Date', 'User', 'Entry_id', 'Energy', 'Total',    'Cost', 'Vazio', 'Cheio', 'Ponta', 'TAR'])
            df.set_index('Date', inplace=True)
        return df

    def Save_Data(self):
        self.EoT_masterdata.index = pd.to_datetime(self.EoT_masterdata.index)
        self.EoT_masterdata.sort_index(inplace=True)
        self.EoT_masterdata.reset_index().to_feather(self.eot_datafile)

    def Read_Config(self):
        config_dict = {}
        with open(self.config_file, 'r') as file:
            config_list = json.load(file)
            for config in config_list:
                user_id = config['userID']
                config_dict[user_id] = {
                    'api_key': config['api_key'],
                    'energy_cost_option': config['energy_cost_option'],
                    'energy_supplier': config['energy_supplier'],
                    'cycle_day': config['cycle_day'],
                    'start_date': config['start_date'],
                    'end_date': config['end_date']
                }
        return config_dict

    def GetEoTData(self, optional_start_date = None, optional_end_date = None):
        self.logger.info (f"GetEoTData: Calling EOT API")

        changeFlag = False
        for user_id, user_config in self.config_dict.items():
            # Retrieve the last date in the dataframe for api_key
            if not self.EoT_masterdata.empty and user_id in self.EoT_masterdata['User'].unique():
                start_date = pd.to_datetime( self.EoT_masterdata[self.EoT_masterdata['User'] == user_id].index.max() )
            else:
                start_date = datetime.datetime.combine(datetime.date.today(), datetime.time(0, 0))

            # Check for optional start and end dates
            start_date = pd.to_datetime( optional_start_date ) if (optional_start_date != None) else start_date
            end_date = pd.to_datetime( optional_end_date ) if (optional_end_date != None) else datetime.datetime.now()

            data = None
            num_blocks = self.count_15min_periods_between_dates(start_date, end_date)
            if (num_blocks > 0):
                # Get Energy Consumption records
                data = self.fetch_EOT_Data( user_config['api_key'], num_blocks, 'tiae')

            if (data != None):
                for feed in data['channels'][0]['feeds']:
                    # Convert 'date_row' to datetime
                    date_row = pd.to_datetime(feed['created_at'], format="%Y-%m-%d %H:%M:%S %Z")
                    # Convert to local timezone and remove it
                    date_row = date_row.tz_convert(pytz.timezone('Europe/Lisbon')).tz_localize(None)
                    # Remove seconds from 'created_at' and format the date as year-month-day hour:minute
                    date_row = date_row.replace(second=0, microsecond=0).strftime("%Y-%m-%d %H:%M")
                    
                    if date_row not in self.EoT_masterdata.index:
                        self.EoT_masterdata.loc[date_row] = [user_id, feed['entry_id'], feed['dif'], feed['total'], np.nan, np.nan, np.nan, np.nan, np.nan ]
                        changeFlag = True

                if (changeFlag):
                    self.EoT_masterdata.index = pd.to_datetime(self.EoT_masterdata.index)
                    self.EoT_masterdata.sort_index(inplace=True)
                    self.CalculateCosts ( user_config['energy_cost_option'], user_config['energy_supplier'], user_config['cycle_day'] )
            
            # Get Last Power record
            power_data = self.fetch_EOT_Data( user_config['api_key'], 1, 'iap_diff')
            if (power_data is None) or (power_data == -1):
                self.logger.error("Error calling EOT API for Power records")
                break
            
            self.lastEoTdate_power = power_data['channels'][0]['feeds'][0]['created_at']
            self.lastEoTpower = power_data['channels'][0]['feeds'][0]['value']

        # Save the data to a Feather file
        if (changeFlag):
            self.Save_Data ()

        # Reset the timer
        self.timer = threading.Timer(self.scheduled_refresh_time, self.GetEoTData)
        self.timer.start()

    def fetch_EOT_Data(self, api_key, results=1, channel='tiae'):
        # Fetch data from EOT API
        self.logger.info (f"fetch_EOT_Data: Fetching data from EOT API. Results: {results}, Channel: {channel}")

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
    
    def count_15min_periods_between_dates(self, start_date, end_date):
        # Helper function to count the number of 15 minute periods between two dates
        # It takes two date parameters and validates them
        if not isinstance(start_date, datetime.datetime) or not isinstance(end_date, datetime.datetime):
            raise ValueError("Both start_date and end_date must be datetime objects")
        if start_date > end_date:
            raise ValueError("start_date must be less than or equal to end_date")
        
        time_diff = end_date - start_date
        periods = time_diff.total_seconds() // (15 * 60)
        return int(periods+1)

    def CalculateCosts (self, option, supplier, cycle_day):
        energy_cost = EnergyCosts(option, supplier, cycle_day, 'BTN-C')
        energy_cost.setMasterPrices(self.master_file)
        self.EoT_masterdata = energy_cost.add_energy_consumption_cost_column ( self.EoT_masterdata, 'Energy')

    def get_current_cycle_period(self, cycle_day, end_date = datetime.datetime.now() ):
            # Get the current date and time
            now = end_date
            cycle_day = int (cycle_day)

            # Check if the current day is less than the given day value
            if now.day <= cycle_day:
                # Get the previous month and year
                prev_month = now.month - 1 if now.month > 1 else 12
                prev_year = now.year if now.month > 1 else now.year - 1

                # Create start_date using the previous month, year, and the provided day value
                start_date = datetime.datetime(prev_year, prev_month, cycle_day,0,0,0)
            else:
                # Create start_date using the current year and month, and the provided day value
                start_date = datetime.datetime(now.year, now.month, cycle_day)

            # Create end_date as today's date at midnight
            end_date = datetime.datetime(now.year, now.month, now.day, now.hour, 0, 0)

            return start_date, end_date

    def GetTotalsForPeriod (self, user_id, start_date, end_date):
        # Filter the rows from start_date to end_date and for the specified user_id
        mask = (self.EoT_masterdata.index >= start_date) & (self.EoT_masterdata.index <= end_date) & (self.EoT_masterdata['User'] == user_id)
        EoT_cost_data_today = self.EoT_masterdata.loc[mask]
        
        # Sum the 'Energy' column of the filtered rows
        total_energy    = EoT_cost_data_today['Energy'].sum()
        total_cost      = EoT_cost_data_today['Cost'].sum()
        last_val        =EoT_cost_data_today['Total'].max()

        return total_energy, total_cost, last_val
    
    def GetCurrentDayTotal(self, user_id):
        # Get the current date and time
        current_now = datetime.datetime.now()
        
        # Create a datetime object for today's midnight
        current_date = datetime.datetime(current_now.year, current_now.month, current_now.day, 0,0)
        
        # Use the GetTotalsForPeriod function to get the total energy and cost for the current day
        current_day_total_energy, current_day_total_cost, last_val = self.GetTotalsForPeriod(user_id, current_date, current_now)
        return current_day_total_energy, current_day_total_cost, last_val

    def GetCurrentMonthTotal(self, user_id, cycle_day):
        # Get the start and end dates for the current cycle period
        start_date, end_date = self.get_current_cycle_period(cycle_day)

        # Use the GetTotalsForPeriod function to get the total energy and cost for the current cycle period
        current_month_total_energy, current_month_total_cost, last_val = self.GetTotalsForPeriod(user_id, start_date, end_date)

        # Return the total energy and cost for the current cycle period
        return current_month_total_energy, current_month_total_cost, last_val
