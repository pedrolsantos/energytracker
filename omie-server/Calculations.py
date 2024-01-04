import math
import datetime
from datetime import time
import pytz
import pandas as pd
import json
from logger_config import setup_logger, setBasePath

#######   TEMPLATE DE CUSTOS DE ENERGIA   #######
Energy_Time_Cycle = {
    'Simples': [
        {'dst':1, 'dias':0, 'start': '00:00', 'end': '23:59', 'periodo':'Cheio'}, # Seg a Sexta 
        {'dst':1, 'dias':1, 'start': '00:00', 'end': '23:59', 'periodo':'Cheio'}, # Sábado
        {'dst':1, 'dias':2, 'start': '00:00', 'end': '23:59', 'periodo':'Cheio'}, # Domingo

        {'dst':0, 'dias':0, 'start': '00:00', 'end': '23:59', 'periodo':'Cheio'}, # Seg a Sexta 
        {'dst':0, 'dias':1, 'start': '00:00', 'end': '23:59', 'periodo':'Cheio'}, # Sábado
        {'dst':0, 'dias':2, 'start': '00:00', 'end': '23:59', 'periodo':'Cheio'}, # Domingo
    ],
    'Bi-Horario-Semanal': [
        {'dst':1, 'dias':0, 'start': '00:00', 'end': '06:59', 'periodo':'Vazio'}, # Seg a Sexta em Vazio no Verão
        {'dst':1, 'dias':0, 'start': '07:00', 'end': '23:59', 'periodo':'Cheio'}, # Seg a Sexta em Fora de Vazio no Verão
        {'dst':1, 'dias':1, 'start': '00:00', 'end': '08:59', 'periodo':'Vazio'}, # Sábado em Vazio no Verão
        {'dst':1, 'dias':1, 'start': '09:00', 'end': '13:59', 'periodo':'Cheio'}, # Sábado em Fora Vazio no Verão
        {'dst':1, 'dias':1, 'start': '14:00', 'end': '19:59', 'periodo':'Vazio'}, # Sábado em Vazio no Verão
        {'dst':1, 'dias':1, 'start': '20:00', 'end': '21:59', 'periodo':'Cheio'}, # Sábado em Fora Vazio no Verão
        {'dst':1, 'dias':1, 'start': '22:00', 'end': '23:59', 'periodo':'Vazio'}, # Sábado em Vazio no Verão
        {'dst':1, 'dias':2, 'start': '00:00', 'end': '23:59', 'periodo':'Vazio'}, # Domingo em Vazio no Verão

        {'dst':0, 'dias':0, 'start': '00:00', 'end': '06:59', 'periodo':'Vazio'}, # Seg a Sexta em Vazio no Inverno
        {'dst':0, 'dias':0, 'start': '07:00', 'end': '23:59', 'periodo':'Cheio'}, # Seg a Sexta em Fora de Vazio no Inverno
        {'dst':0, 'dias':1, 'start': '00:00', 'end': '09:29', 'periodo':'Vazio'}, # Sábado em Vazio no Inverno
        {'dst':0, 'dias':1, 'start': '09:30', 'end': '12:59', 'periodo':'Cheio'}, # Sábado em Fora Vazio no Inverno
        {'dst':0, 'dias':1, 'start': '13:00', 'end': '18:29', 'periodo':'Vazio'}, # Sábado em Vazio no Inverno
        {'dst':0, 'dias':1, 'start': '18:30', 'end': '21:59', 'periodo':'Cheio'}, # Sábado em Fora Vazio no Inverno
        {'dst':0, 'dias':1, 'start': '22:00', 'end': '23:59', 'periodo':'Vazio'}, # Sábado em Vazio no Inverno
        {'dst':0, 'dias':2, 'start': '00:00', 'end': '23:59', 'periodo':'Vazio'}, # Domingo em Vazio no Inverno
    ],
    'Bi-Horario-Diario': [
        {'dst':1, 'dias':0, 'start': '00:00', 'end': '07:59', 'periodo':'Vazio'}, # Seg a Sexta em Vazio no Verão
        {'dst':1, 'dias':0, 'start': '08:00', 'end': '21:59', 'periodo':'Cheio'}, # Seg a Sexta em Fora de Vazio no Verão
        {'dst':1, 'dias':0, 'start': '22:00', 'end': '23:59', 'periodo':'Vazio'}, # Seg a Sexta em Vazio no Verão

        {'dst':1, 'dias':1, 'start': '00:00', 'end': '07:59', 'periodo':'Vazio'}, # Sábado em Vazio no Verão
        {'dst':1, 'dias':1, 'start': '08:00', 'end': '21:59', 'periodo':'Cheio'}, # Sábado em Fora de Vazio no Verão
        {'dst':1, 'dias':1, 'start': '22:00', 'end': '23:59', 'periodo':'Vazio'}, # Sábado em Vazio no Verão

        {'dst':1, 'dias':2, 'start': '00:00', 'end': '07:59', 'periodo':'Vazio'}, # Domingo em Vazio no Verão
        {'dst':1, 'dias':2, 'start': '08:00', 'end': '21:59', 'periodo':'Cheio'}, # Domingo em Fora de Vazio no Verão
        {'dst':1, 'dias':2, 'start': '22:00', 'end': '23:59', 'periodo':'Vazio'}, # Domingo em Vazio no Verão

        {'dst':0, 'dias':0, 'start': '00:00', 'end': '07:59', 'periodo':'Vazio'}, # Seg a Sexta em Vazio no Inverno
        {'dst':0, 'dias':0, 'start': '08:00', 'end': '21:59', 'periodo':'Cheio'}, # Seg a Sexta em Fora de Vazio no Inverno
        {'dst':0, 'dias':0, 'start': '22:00', 'end': '23:59', 'periodo':'Vazio'}, # Seg a Sexta em Vazio no Inverno

        {'dst':0, 'dias':1, 'start': '00:00', 'end': '07:59', 'periodo':'Vazio'}, # Sábado em Vazio no Inverno
        {'dst':0, 'dias':1, 'start': '08:00', 'end': '21:59', 'periodo':'Cheio'}, # Sábado em Fora de Vazio no Inverno
        {'dst':0, 'dias':1, 'start': '22:00', 'end': '23:59', 'periodo':'Vazio'}, # Sábado em Vazio no Inverno

        {'dst':0, 'dias':2, 'start': '00:00', 'end': '07:59', 'periodo':'Vazio'}, # Domingo em Vazio no Inverno
        {'dst':0, 'dias':2, 'start': '08:00', 'end': '21:59', 'periodo':'Cheio'}, # Domingo em Fora de Vazio no Inverno
        {'dst':0, 'dias':2, 'start': '22:00', 'end': '23:59', 'periodo':'Vazio'}, # Domingo em Vazio no Inverno
    ],
    'Tri-Horario-Semanal': [
        {'dst':1, 'dias':0, 'start': '00:00', 'end': '06:59', 'periodo':'Vazio'}, # Seg a Sexta em Vazio no Verão
        {'dst':1, 'dias':0, 'start': '07:00', 'end': '09:14', 'periodo':'Cheio'}, # Seg a Sexta em Cheio no Verão
        {'dst':1, 'dias':0, 'start': '09:15', 'end': '12:14', 'periodo':'Ponta'}, # Seg a Sexta em Ponta no Verão
        {'dst':1, 'dias':0, 'start': '12:15', 'end': '23:59', 'periodo':'Cheio'}, # Seg a Sexta em Cheio no Verão
        {'dst':1, 'dias':1, 'start': '00:00', 'end': '08:59', 'periodo':'Vazio'}, # Sábado em Vazio no Verão
        {'dst':1, 'dias':1, 'start': '09:00', 'end': '13:59', 'periodo':'Cheio'}, # Sábado em Cheio no Verão
        {'dst':1, 'dias':1, 'start': '14:00', 'end': '19:59', 'periodo':'Vazio'}, # Sábado em Vazio no Verão
        {'dst':1, 'dias':1, 'start': '20:00', 'end': '21:59', 'periodo':'Cheio'}, # Sábado em Cheio no Verão
        {'dst':1, 'dias':1, 'start': '22:00', 'end': '23:59', 'periodo':'Vazio'}, # Sábado em Vazio no Verão
        {'dst':1, 'dias':2, 'start': '00:00', 'end': '23:59', 'periodo':'Vazio'}, # Domingo em Vazio no Verão

        {'dst':0, 'dias':0, 'start': '00:00', 'end': '06:59', 'periodo':'Vazio'}, # Seg a Sexta em Vazio no Inverno
        {'dst':0, 'dias':0, 'start': '07:00', 'end': '09:29', 'periodo':'Cheio'}, # Seg a Sexta em Cheio no Inverno
        {'dst':0, 'dias':0, 'start': '09:30', 'end': '11:59', 'periodo':'Ponta'}, # Seg a Sexta em Ponta no Inverno
        {'dst':0, 'dias':0, 'start': '12:00', 'end': '18:29', 'periodo':'Cheio'}, # Seg a Sexta em Cheio no Inverno
        {'dst':0, 'dias':0, 'start': '18:30', 'end': '20:59', 'periodo':'Ponta'}, # Seg a Sexta em Ponta no Inverno
        {'dst':0, 'dias':0, 'start': '21:00', 'end': '23:59', 'periodo':'Cheio'}, # Seg a Sexta em Cheio no Inverno
        {'dst':0, 'dias':1, 'start': '00:00', 'end': '09:29', 'periodo':'Vazio'}, # Sábado em Vazio no Inverno
        {'dst':0, 'dias':1, 'start': '09:30', 'end': '12:59', 'periodo':'Cheio'}, # Sábado em Cheio no Inverno
        {'dst':0, 'dias':1, 'start': '13:00', 'end': '18:29', 'periodo':'Vazio'}, # Sábado em Vazio no Inverno
        {'dst':0, 'dias':1, 'start': '18:30', 'end': '21:59', 'periodo':'Cheio'}, # Sábado em Cheio no Inverno
        {'dst':0, 'dias':1, 'start': '22:00', 'end': '23:59', 'periodo':'Vazio'}, # Sábado em Vazio no Inverno
        {'dst':0, 'dias':2, 'start': '00:00', 'end': '23:59', 'periodo':'Vazio'}, # Domingo em Vazio no Inverno
    ],
    'Tri-Horario-Diario': [
        {'dst':1, 'dias':0, 'start': '00:00', 'end': '07:59', 'periodo':'Vazio'}, # Seg a Sexta em Vazio no Verão
        {'dst':1, 'dias':0, 'start': '08:00', 'end': '10:29', 'periodo':'Cheio'}, # Seg a Sexta em Cheio no Verão
        {'dst':1, 'dias':0, 'start': '10:30', 'end': '12:59', 'periodo':'Ponta'}, # Seg a Sexta em Ponta no Verão
        {'dst':1, 'dias':0, 'start': '13:00', 'end': '19:29', 'periodo':'Cheio'}, # Seg a Sexta em Cheio no Verão
        {'dst':1, 'dias':0, 'start': '19:30', 'end': '20:59', 'periodo':'Ponta'}, # Seg a Sexta em Ponta no Verão
        {'dst':1, 'dias':0, 'start': '21:00', 'end': '21:59', 'periodo':'Cheio'}, # Seg a Sexta em Cheio no Verão
        {'dst':1, 'dias':0, 'start': '22:00', 'end': '23:59', 'periodo':'Vazio'}, # Seg a Sexta em Vazio no Verão

        {'dst':1, 'dias':1, 'start': '00:00', 'end': '07:59', 'periodo':'Vazio'}, # Sabado em Vazio no Verão
        {'dst':1, 'dias':1, 'start': '08:00', 'end': '10:29', 'periodo':'Cheio'}, # Sabado em Cheio no Verão
        {'dst':1, 'dias':1, 'start': '10:30', 'end': '12:59', 'periodo':'Ponta'}, # Sabado em Ponta no Verão
        {'dst':1, 'dias':1, 'start': '13:00', 'end': '19:29', 'periodo':'Cheio'}, # Sabado em Cheio no Verão
        {'dst':1, 'dias':1, 'start': '19:30', 'end': '20:59', 'periodo':'Ponta'}, # Sabado em Ponta no Verão
        {'dst':1, 'dias':1, 'start': '21:00', 'end': '21:59', 'periodo':'Cheio'}, # Sabado em Cheio no Verão
        {'dst':1, 'dias':1, 'start': '22:00', 'end': '23:59', 'periodo':'Vazio'}, # Sabado em Vazio no Verão

        {'dst':1, 'dias':2, 'start': '00:00', 'end': '07:59', 'periodo':'Vazio'}, # Domingo em Vazio no Verão
        {'dst':1, 'dias':2, 'start': '08:00', 'end': '10:29', 'periodo':'Cheio'}, # Domingo em Cheio no Verão
        {'dst':1, 'dias':2, 'start': '10:30', 'end': '12:59', 'periodo':'Ponta'}, # Domingo em Ponta no Verão
        {'dst':1, 'dias':2, 'start': '13:00', 'end': '19:29', 'periodo':'Cheio'}, # Domingo em Cheio no Verão
        {'dst':1, 'dias':2, 'start': '19:30', 'end': '20:59', 'periodo':'Ponta'}, # Domingo em Ponta no Verão
        {'dst':1, 'dias':2, 'start': '21:00', 'end': '21:59', 'periodo':'Cheio'}, # Domingo em Cheio no Verão
        {'dst':1, 'dias':2, 'start': '22:00', 'end': '23:59', 'periodo':'Vazio'}, # Domingo em Vazio no Verão

        {'dst':0, 'dias':0, 'start': '00:00', 'end': '07:59', 'periodo':'Vazio'}, # Seg a Sexta em Vazio no Inverno
        {'dst':0, 'dias':0, 'start': '08:00', 'end': '08:59', 'periodo':'Cheio'}, # Seg a Sexta em Cheio no Inverno
        {'dst':0, 'dias':0, 'start': '09:00', 'end': '10:29', 'periodo':'Ponta'}, # Seg a Sexta em Ponta no Inverno
        {'dst':0, 'dias':0, 'start': '10:30', 'end': '17:59', 'periodo':'Cheio'}, # Seg a Sexta em Cheio no Inverno
        {'dst':0, 'dias':0, 'start': '18:00', 'end': '20:29', 'periodo':'Ponta'}, # Seg a Sexta em Ponta no Inverno
        {'dst':0, 'dias':0, 'start': '20:30', 'end': '21:59', 'periodo':'Cheio'}, # Seg a Sexta em Cheio no Inverno
        {'dst':0, 'dias':0, 'start': '22:00', 'end': '23:59', 'periodo':'Vazio'}, # Seg a Sexta em Vazio no Inverno

        {'dst':0, 'dias':1, 'start': '00:00', 'end': '07:59', 'periodo':'Vazio'}, # Sabado em Vazio no Inverno
        {'dst':0, 'dias':1, 'start': '08:00', 'end': '08:59', 'periodo':'Cheio'}, # Sabado em Cheio no Inverno
        {'dst':0, 'dias':1, 'start': '09:00', 'end': '10:29', 'periodo':'Ponta'}, # Sabado em Ponta no Inverno
        {'dst':0, 'dias':1, 'start': '10:30', 'end': '17:59', 'periodo':'Cheio'}, # Sabado em Cheio no Inverno
        {'dst':0, 'dias':1, 'start': '18:00', 'end': '20:29', 'periodo':'Ponta'}, # Sabado em Ponta no Inverno
        {'dst':0, 'dias':1, 'start': '20:30', 'end': '21:59', 'periodo':'Cheio'}, # Sabado em Cheio no Inverno
        {'dst':0, 'dias':1, 'start': '22:00', 'end': '23:59', 'periodo':'Vazio'}, # Sabado em Vazio no Inverno

        {'dst':0, 'dias':2, 'start': '00:00', 'end': '07:59', 'periodo':'Vazio'}, # Domingo em Vazio no Inverno
        {'dst':0, 'dias':2, 'start': '08:00', 'end': '08:59', 'periodo':'Cheio'}, # Domingo em Cheio no Inverno
        {'dst':0, 'dias':2, 'start': '09:00', 'end': '10:29', 'periodo':'Ponta'}, # Domingo em Ponta no Inverno
        {'dst':0, 'dias':2, 'start': '10:30', 'end': '17:59', 'periodo':'Cheio'}, # Domingo em Cheio no Inverno
        {'dst':0, 'dias':2, 'start': '18:00', 'end': '20:29', 'periodo':'Ponta'}, # Domingo em Ponta no Inverno
        {'dst':0, 'dias':2, 'start': '20:30', 'end': '21:59', 'periodo':'Cheio'}, # Domingo em Cheio no Inverno
        {'dst':0, 'dias':2, 'start': '22:00', 'end': '23:59', 'periodo':'Vazio'}, # Domingo em Vazio no Inverno

    ]
}

###### TARIFAS ######
ENERGY_TAR_PERIODS = [ 
    {'start': '2023-01-01', 'end': '2023-06-30', 'Simples':-0.0958, 'Bi-Horario': {'Vazio':-0.1185, 'Cheio':-0.0842}, 'Tri-Horario': {'Vazio':-0.1185, 'Cheio':-0.1069, 'Ponta':-0.0018} },
    {'start': '2023-07-01', 'end': '2024-01-01', 'Simples':-0.0121, 'Bi-Horario': {'Vazio':-0.0349, 'Cheio':-0.0005}, 'Tri-Horario': {'Vazio':-0.0349, 'Cheio':-0.0232, 'Ponta': 0.0818} },
    {'start': '2024-01-01', 'end': '2025-01-01', 'Simples': 0.0365, 'Bi-Horario': {'Vazio': 0.0502, 'Cheio': 0.0092}, 'Tri-Horario': {'Vazio': 0.0092, 'Cheio': 0.0237, 'Ponta': 0.1511} },
]

###### FORNECEDORES DE ENERGIA - CUSTOS ######
ENERGY_SUPPLIERS_PARAMETERS = {
    'coopernico-base': [
        {'start': '2023-01-01', 'end': '2024-01-01', 'cgs': 0.004, 'k':0.01, 'go': 0 },
        {'start': '2024-01-01', 'end': '2025-01-01', 'cgs': 0.004, 'k':0.01, 'go': 0 }
    ],
    'coopernico-go': [
        {'start': '2023-01-01', 'end': '2024-01-01', 'cgs': 0.004, 'k':0.01, 'go': 0.01 },
        {'start': '2024-01-01', 'end': '2025-01-01', 'cgs': 0.004, 'k':0.01, 'go': 0.005 }
    ],
    'luzboa-spot': [
        {'start': '2023-01-01', 'end': '2024-01-01', 'saj': 0.004, 'custos_gestao':0.005, 'fator_adequacao':1.02 },
        {'start': '2024-01-01', 'end': '2025-01-01', 'saj': 0.004, 'custos_gestao':0.005, 'fator_adequacao':1.02 }
    ]
}

###### PERFIS DE CONSUMO ######
ENERGY_PROFILES = ['BTN-A', 'BTN-B', 'BTN-C', 'IP', 'mP', 
                   'UPAC-A-CV-Consumo', 'UPAC-B-CV-Consumo', 'UPAC-C-CV-Consumo', 
                   'UPAC-A-Consumo', 'UPAC-B-Consumo', 'UPAC-C-Consumo']

### CLASSES ###
class EnergyCosts():
    logger = None
    timezone = None
    energy_cost_option = None
    energy_supplier = None
    cycle_day = None
    profile = None
    master_prices_table = None
    luzboa_prices = None


    def __init__(self, option, supplier, cycle_day=1, profile= 'BTN-C', timezone = 'Europe/Lisbon'):
        self.logger = setup_logger('EnergyCosts')
        self.timezone = timezone

        self.energy_cost_option = option
        self.energy_supplier = supplier
        self.cycle_day = cycle_day
        self.profile = profile
        

    def count_15min_periods_today(self):
        # Helper function to count the number of 15 minute periods since midnight
        # This is used when calling the EoT API to get the energy consumed
        now = datetime.datetime.utcnow()
        midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
        time_diff = now - midnight
        periods = time_diff.seconds // (15 * 60)
        return (periods+1)

    def get_dst_dates(self, year, timezone):
        """
        This function calculates the start and end dates of daylight savings time for a given year and timezone.
    
        Args:
            year (int): The year for which to calculate the daylight savings time dates.
            timezone (str): The timezone in which to calculate the daylight savings time dates.
    
        Returns:
            tuple: A tuple containing the start and end dates of daylight savings time for the given year and timezone.
        """
        tz = pytz.timezone(timezone)

        # Last Sunday of March
        dst_start = datetime.datetime(year, 3, 31, 2, 0, tzinfo=tz)
        while dst_start.weekday() != 6:  # 6 represents Sunday
            dst_start -= datetime.timedelta(days=1)

        # Last Sunday of October
        dst_end = datetime.datetime(year, 10, 30, 2, 0, tzinfo=tz)
        while dst_end.weekday() != 6:  # 6 represents Sunday
            dst_end -= datetime.timedelta(days=1)

        # Remove timezone information
        dst_start = dst_start.replace(tzinfo=None)
        dst_end = dst_end.replace(tzinfo=None)
        return dst_start, dst_end

    def is_daylight_savings(self, date):
        year = date.year
        dst_start, dst_end = self.get_dst_dates (year, self.timezone)
        return dst_start <= date < dst_end

    def get_day_category(self, date):
        day = date.weekday()
        if day >= 0 and day <= 4:  # Weekday (Monday=0, Tuesday=1, ..., Friday=4)
            return 0
        elif day == 5:  # Saturday
            return 1
        elif day == 6:  # Sunday
            return 2

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

    def get_omie_price_for_date (self, dataframe, str_time, price_field='Price_PT', lagHour=1):
        target_time = pd.to_datetime(str_time)

        # hack to account for lag in the OMIE data being in ES timeframe
        if (lagHour > 0):
            target_time= target_time + pd.Timedelta(hours=lagHour)

        # Filter the DataFrame for rows with index values less than the target time
        filtered_df = dataframe[dataframe.index <= target_time]

        # Check if the filtered DataFrame is empty
        if filtered_df.empty:
            # Return the first row of the original DataFrame
            nearest_row = dataframe.iloc[0]
        else:
            # Select the last row of the filtered DataFrame
            nearest_row = filtered_df.iloc[-1]

        omie_price = nearest_row[price_field]
        omie_date = nearest_row.name

        ret_data = { 
                'OMIE Date': omie_date.strftime('%Y-%m-%d %H:%M'), 
                'OMIE Price': omie_price, 
                'LagHour': lagHour
        }

        return omie_price, ret_data

    def get_price_tar (self, str_date, cost_option = None):
        date = pd.to_datetime(str_date)
        hour_minute = date.time()
        date_dst = 1 if self.is_daylight_savings(date) else 0
        date_category = self.get_day_category(date)

        if cost_option == None:
            time_plan = self.energy_cost_option
        else:
            time_plan = cost_option
        option = 'Bi-Horario' if ('Bi' in time_plan) else 'Tri-Horario' if ('Tri' in time_plan) else 'Simples'

        tar = 0 # Default value
        break_all_flag = False 
        for period in Energy_Time_Cycle[time_plan]:
            if (period['dst'] == date_dst):
                if (period['dias'] == date_category):
                    start_time = datetime.datetime.strptime(period['start'], '%H:%M').time()
                    end_time = datetime.datetime.strptime(period['end'], '%H:%M').time()
                    if start_time <= hour_minute <= end_time:

                        for cycle in ENERGY_TAR_PERIODS:
                            start_time = datetime.datetime.strptime(cycle['start'], '%Y-%m-%d')
                            end_time = datetime.datetime.strptime(cycle['end'], '%Y-%m-%d')
                            if start_time <= date <= end_time:
                                break_all_flag = True
                                if (option == 'Simples'):
                                    tar = cycle[option]
                                else:
                                    tar = cycle[option][period['periodo']]
                                break
                        
                        if (break_all_flag):
                            break
        return tar, period, period['periodo']

    def get_loss_profile_for_period (self, loss_profile_data, start_date, end_date):
        start_date = pd.to_datetime(start_date)
        end_date = pd.to_datetime(end_date)
        loss = 0
            
        # Filter the Loss data for the period
        period_df = loss_profile_data.loc[start_date:end_date].copy ()

        # Add columns for "Vazio", "Cheio" or "Ponta" considering Energy = 1 
        period_df['Total_Loss'] = ((1 + period_df['BT']) * (1 + period_df['MT']) * (1 + period_df['AT']) * (1 + period_df['ATRNT'])) - 1
        period_df[['Vazio', 'Cheio', 'Ponta', 'TAR']] = period_df.apply(lambda row: self.add_columns_energy_by_period(row, column_name='Total_Loss'), axis=1)

        fv_sum =  period_df[ ['Cheio', 'Ponta']].sum().sum()
        fv_count =  period_df[ ['Cheio', 'Ponta']].count().sum() 

        # Calculate the average price for each period
        data = {
            'avgVazio': period_df['Vazio'].mean(),
            'avgCheio': period_df['Cheio'].mean(),
            'avgPonta': period_df['Ponta'].mean(),
            'avgForaVazio': fv_sum / fv_count,
            'avgTotal': period_df['Total_Loss'].mean(),
            'numPeriods': len(period_df),
        }
        return data
        
    def calc_coopernico_price (self, omie_price, str_currdate, fator_perdas_energia=0.15658358, supplier=None):
        # Coopernico Formula : Preço Energia (€/kWh) = (PM + CGS + k + GO) x (1+FP)
        # PM = Preço OMIE / 1000
        # CGS = Custo de Gestão do Sistema
        # k = Margem de Comercialização
        # FP = Fator de Potência
        # https://www.coopernico.pt/pt/energia/precos-energia/precos-energia-2021

        date = pd.to_datetime (str_currdate)
        if supplier == None:
            supplier = self.energy_supplier

        no_data = True
        for period in ENERGY_SUPPLIERS_PARAMETERS[supplier] :
            start_time = datetime.datetime.strptime( period['start'], '%Y-%m-%d')
            end_time = datetime.datetime.strptime( period['end'], '%Y-%m-%d')
            
            if start_time <= date <= end_time:
                custos_gestao = period['cgs']
                margem = period['k']
                cost_GO = period['go']
                no_data = False
                break

        if no_data:
            # no data exist for the current date
            return 0;

        price = (omie_price/1000 + custos_gestao + margem + cost_GO) * (1+fator_perdas_energia) 
        return price

    def calc_luzboa_price (self, preco_medio_mensal, fator_perdas_energia, saj=0.004, fator_adequacao=1.02, custos_gestao=0.005 ):
        # LuzBoa: https://luzboa.pt/wp-content/uploads/2023/01/LUZBOA-SPOT_FAQ_2023.pdf
        # CE = ER x PFC x (1+PT) x FA + (ERxCG) 
        # PFC = (PMD + (Desvio+SAJ) )
        
        price = (preco_medio_mensal + saj) * (1+fator_perdas_energia) * fator_adequacao + custos_gestao 
        return price

    def update_luzboa_cache_prices (self):
        start_date, end_date = self.get_current_cycle_period(self.cycle_day)
        self.luzboa_prices = self.get_luzboa_average_prices (start_date, end_date)

    def calc_energy_price (self, date, energy_consumption, divisionFactor=1):
        # Check if the energy consumption is NaN
        if (math.isnan(energy_consumption)):
            return 0, {}
        
        # Get the Columns of Master Table for the Options on Self
        supplier_column, tar_column, tar_period_column = self.get_master_table_columns_from_options()

        ## Get the row data from Master Table
        sub_data = self.master_prices_table.loc[:date].iloc[-1]
        
        # Get the necessary data
        price = sub_data[supplier_column]
        price_tar = sub_data[tar_column]
        period = sub_data[tar_period_column]

        if (self.energy_supplier == 'luzboa-spot'):
            self.update_luzboa_cache_prices()
            if (self.luzboa_prices is not None):
                if (self.energy_cost_option == 'Simples'):
                    price = self.luzboa_prices['simples']
                    price_tar = 0
                elif 'Bi-Horario' in self.energy_cost_option:
                    price = self.luzboa_prices['avgVazio'] if (period['periodo'] == 'Vazio') else self.luzboa_prices['avgForaVazio']
                    price_tar = 0
                elif 'Tri-Horario' in self.energy_cost_option:
                    price = self.luzboa_prices['avgVazio'] if (period['periodo'] == 'Vazio') else self.luzboa_prices['avgCheio'] if (period['periodo'] == 'Cheio') else self.luzboa_prices['avgPonta']
                    price_tar = 0

        # Calculate the energy cost
        cost = (energy_consumption / divisionFactor) * (price + price_tar)
        return cost 

    def get_luzboa_average_prices (self, start_date, end_date, lagHour=1):
        # Convert the dates to datetime
        start_date = pd.to_datetime(start_date) - datetime.timedelta(hours=lagHour) #LuzBoa is not using PT time
        end_date = pd.to_datetime(end_date) - datetime.timedelta(hours=lagHour)

        # Filter the master_table for the period
        working_table = self.master_prices_table.loc[start_date:end_date].copy()

        # init variables:
        price_simples = 0
        price_cheio = 0
        price_vazio = 0
        price_ponta = 0
        price_fora_vazio = 0

        if 'Simples' in self.energy_cost_option:
            # USE Mean values for the period
            avg_Loss = working_table['Loss'].mean()
            avg_OMIE_Price = working_table['OMIE Price'].resample('H').mean().mean() # working_table['OMIE Price'].mean()
            tar = self.get_price_tar (start_date)[0]
            price_simples = self.calc_luzboa_price (avg_OMIE_Price, fator_perdas_energia=avg_Loss) + tar
        else: # Bi-Horario or Tri-Horario
            # Calculate the price for each row
            working_table['Price'] = working_table.apply(lambda row: self.calc_luzboa_price (row['OMIE Price'] , fator_perdas_energia= row['Loss']), axis=1)

            # Add columns for "Vazio", "Cheio" or "Ponta"
            working_table[['Vazio', 'Cheio', 'Ponta', 'TAR']] = working_table.apply(lambda x: self.add_columns_energy_by_period (x, self.profile), axis=1)

            # Calc the net price column
            working_table['PriceNET'] = working_table['Price'] + working_table['TAR']
        
            price_vazio =  working_table.loc[ ~working_table['Vazio'].isna(), 'PriceNET'].resample('H').mean().mean() 
            price_fora_vazio = working_table.loc[ working_table['Vazio'].isna(), 'PriceNET'].resample('H').mean().mean() 
            
            price_cheio = working_table.loc[ ~working_table['Cheio'].isna(), 'PriceNET'].resample('H').mean().mean()
            price_ponta = working_table.loc[ ~working_table['Ponta'].isna(), 'PriceNET'].resample('H').mean().mean() 

            price_cheio = 0 if pd.isna(price_cheio) else price_cheio
            price_ponta = 0 if pd.isna(price_ponta) else price_ponta
        

        # Calculate the average price for each period
        data = {
            'simples': price_simples,
            'avgVazio': price_vazio,
            'avgForaVazio': price_fora_vazio,
            'avgCheio': price_cheio,
            'avgPonta': price_ponta,
            'avgOmiePrice': working_table['OMIE Price'].resample('H').mean().mean(),
            'numPeriods': len(working_table),
        }
        return data

    def setMasterPrices (self, master_prices_table):
        self.master_prices_table = master_prices_table.copy()

    def add_columns_energy_by_period (self, row, column_name= 'Energy'):
        date = row.name
        energy = row[column_name]

        # Get the price tar
        price_tar, period, _ = self.get_price_tar ( date )

        if period['periodo'] =='Vazio':
            return pd.Series( [energy, None, None, price_tar], index=['Vazio', 'Cheio', 'Ponta', 'TAR'] )
        elif period['periodo'] =='Cheio':
            return pd.Series( [None, energy, None, price_tar], index=['Vazio', 'Cheio', 'Ponta', 'TAR'] )
        elif period['periodo'] =='Ponta':
            return pd.Series( [None, None, energy, price_tar], index=['Vazio', 'Cheio', 'Ponta', 'TAR'] )
        
        # If the period is not defined, return None
        return pd.Series( [None, None, None, None], index=['Vazio', 'Cheio', 'Ponta', 'TAR'] )

    def add_energy_consumption_cost_column (self, energy_data, energy_column= 'Energy', divisionFactor=1):
        # Calculate the energy price for each row
        energy_data = energy_data.copy()

        # Check if 'Cost' column exists
        if 'Cost' in energy_data.columns:
            # Only calculate for rows where 'Cost' is NaN
            mask = energy_data['Cost'].isna()
            energy_data.loc[mask, 'Cost'] = energy_data[mask].apply(lambda row: self.calc_energy_price(row.name, row[energy_column], divisionFactor), axis=1)
        else:
            # 'Cost' column doesn't exist, calculate for all rows
            energy_data['Cost'] = energy_data.apply(lambda row: self.calc_energy_price(row.name, row[energy_column], divisionFactor), axis=1)

        if divisionFactor > 1:
            energy_data[energy_column] = energy_data[energy_column] / divisionFactor
        
        # Add the columns 'Vazio', 'Cheio', 'Ponta', and 'TAR' with the energy consumption for each period
        if 'TAR' in energy_data.columns:
            # Only apply function to rows where the columns are NaN
            mask = energy_data['TAR'].isna()
            energy_data.loc[mask, ['Vazio', 'Cheio', 'Ponta', 'TAR']] = energy_data[mask].apply(self.add_columns_energy_by_period, axis=1)
        else:
            # Columns don't exist, apply function to all rows
            energy_data[['Vazio', 'Cheio', 'Ponta', 'TAR']] = energy_data.apply(self.add_columns_energy_by_period, axis=1)

        return energy_data

    def get_profile_estimation(self, profile_data, omie_data, loss_profile_data, start_date, end_date, total_energy, records='none', lagHour=1):
        total_energy= float (total_energy)
        sum_profile_period = profile_data.loc[start_date:end_date][self.profile].sum()
        energy_profile = profile_data.loc[start_date:end_date][self.profile] * (total_energy / sum_profile_period)

        # Convert the Series to a DataFrame and set the column name
        energy_profile_df = energy_profile.to_frame()
        energy_profile_df.columns = ['Energy']

        energy_profile_df = self.add_energy_consumption_cost_column (energy_profile_df)

        recs = ''
        if records == 'csv':
            recs = energy_profile_df.to_csv(index=True, date_format='%Y-%m-%d %H:%M', path_or_buf=None)
        elif records == 'json':
            # Format the index     
            energy_profile_df.index = energy_profile_df.index.strftime('%Y-%m-%d %H:%M')
            # Convert the DataFrame to JSON
            recs = json.loads ( energy_profile_df.to_json( orient='table', index=True) )['data']

        energy_vazio_sum = energy_profile_df['Vazio'].astype(float).sum()
        energy_cheio_sum = energy_profile_df['Cheio'].astype(float).sum()
        energy_ponta_sum = energy_profile_df['Ponta'].astype(float).sum()

        # Sum the Cost of all rows with "Vazio", "Cheio" and "Ponta" not null
        cost_vazio_sum = energy_profile_df.loc[energy_profile_df['Vazio'].notnull(), 'Cost'].astype(float).sum()
        cost_cheio_sum = energy_profile_df.loc[energy_profile_df['Cheio'].notnull(), 'Cost'].astype(float).sum()
        cost_ponta_sum = energy_profile_df.loc[energy_profile_df['Ponta'].notnull(), 'Cost'].astype(float).sum()        

        # Average cost of each
        vazio_avg_cost = cost_vazio_sum / energy_vazio_sum
        cheio_avg_cost = cost_cheio_sum / energy_cheio_sum
        ponta_avg_cost = cost_ponta_sum / energy_ponta_sum

        data_dict = {
            'Start_date' : start_date.strftime("%Y-%d-%m %H:%M"),
            'End_date' : end_date.strftime("%Y-%d-%m %H:%M"),
            'Total_Energy' : (energy_vazio_sum + energy_cheio_sum + energy_ponta_sum),
            'Energy_Vazio' : energy_vazio_sum,
            'Energy_Cheio' : energy_cheio_sum, 
            'Energy_Ponta' : energy_ponta_sum,
            'Total_cost' : (cost_vazio_sum + cost_cheio_sum + cost_ponta_sum),
            'Cost_Vazio' : cost_vazio_sum,
            'Cost_Cheio' : cost_cheio_sum,
            'Cost_Ponta' : cost_ponta_sum,
            'Vazio_avg_price_cost' : vazio_avg_cost,
            'Cheio_avg_price_cost' : cheio_avg_cost,
            'Ponta_avg_price_cost' : ponta_avg_cost,
            'recs' : recs  
        }
        return data_dict

    def calc_luzboa_costs(self, start_date, end_date, vazio_energy, cheio_energy, ponta_energy, lagHour, records= 'none'):
        vazio_energy= float (vazio_energy)
        cheio_energy= float (cheio_energy)
        ponta_energy= float (ponta_energy)

        # Convert the dates to datetime
        start_date = pd.to_datetime(start_date)
        end_date = pd.to_datetime(end_date)
        
        cost_vazio_sum = 0
        cost_cheio_sum = 0
        cost_ponta_sum = 0
        vazio_avg_cost = 0
        cheio_avg_cost = 0
        ponta_avg_cost = 0
        recs =''

        luzboa_prices = self.get_luzboa_average_prices (start_date, end_date, lagHour=lagHour)
        if (self.energy_cost_option == 'Simples'):
            energy_vazio_sum = 0
            energy_cheio_sum = vazio_energy + cheio_energy + ponta_energy
            energy_ponta_sum = 0 
            cost_vazio_sum = 0
            cost_cheio_sum = energy_cheio_sum * luzboa_prices['simples']
            cost_ponta_sum = 0
            vazio_avg_cost = 0
            cheio_avg_cost = luzboa_prices['simples']
            ponta_avg_cost = 0
        elif ('Bi-Horario' in self.energy_cost_option):
            energy_vazio_sum = vazio_energy
            energy_cheio_sum = cheio_energy + ponta_energy
            energy_ponta_sum = 0 
            cost_vazio_sum = energy_vazio_sum * luzboa_prices['avgVazio']
            cost_cheio_sum = energy_cheio_sum * luzboa_prices['avgForaVazio']
            cost_ponta_sum = 0
            vazio_avg_cost = luzboa_prices['avgVazio']
            cheio_avg_cost = luzboa_prices['avgForaVazio']
            ponta_avg_cost = 0
        elif ('Tri-Horario' in self.energy_cost_option):
            energy_vazio_sum = vazio_energy
            energy_cheio_sum = cheio_energy
            energy_ponta_sum = ponta_energy
            cost_vazio_sum = energy_vazio_sum * luzboa_prices['avgVazio']
            cost_cheio_sum = energy_cheio_sum * luzboa_prices['avgCheio']
            cost_ponta_sum = energy_ponta_sum * luzboa_prices['avgPonta']
            vazio_avg_cost = luzboa_prices['avgVazio']
            cheio_avg_cost = luzboa_prices['avgCheio']
            ponta_avg_cost = luzboa_prices['avgPonta']


        data_dict = {
            'Start_date' : start_date.strftime("%Y-%d-%m %H:%M"),
            'End_date' : end_date.strftime("%Y-%d-%m %H:%M"),
            'Total_Energy' : (vazio_energy+cheio_energy+ponta_energy),
            'Energy_Vazio' : energy_vazio_sum,
            'Energy_Cheio' : energy_cheio_sum, 
            'Energy_Ponta' : energy_ponta_sum,
            'Total_cost' : cost_vazio_sum + cost_cheio_sum + cost_ponta_sum,
            'Cost_Vazio' : cost_vazio_sum,
            'Cost_Cheio' : cost_cheio_sum,
            'Cost_Ponta' : cost_ponta_sum,
            'Vazio_avg_price_cost' : vazio_avg_cost,
            'Cheio_avg_price_cost' : cheio_avg_cost,
            'Ponta_avg_price_cost' : ponta_avg_cost,
            'recs' : recs  
        }
        return data_dict

    def calc_coopernico_costs(self, start_date, end_date, vazio_energy, cheio_energy, ponta_energy, lagHour, records= 'none'):
        vazio_energy= float (vazio_energy)
        cheio_energy= float (cheio_energy)
        ponta_energy= float (ponta_energy)

        # Convert the dates to datetime
        start_date = pd.to_datetime(start_date) + datetime.timedelta(hours=lagHour)
        end_date = pd.to_datetime(end_date) + datetime.timedelta(hours=lagHour)

        # Filter the master_table for the period
        working_table = self.master_prices_table.loc[start_date:end_date].copy()

        # Calculate the price for each row
        ## OLD Function : working_table['Price'] = working_table.apply(lambda row: self.calc_coopernico_price (row['OMIE Price']*1000 , fator_perdas_energia= row['Loss'] ), axis=1)
        working_table['Price'] = working_table.apply(lambda row: self.calc_coopernico_price (row['OMIE Price']*1000 , row.name, fator_perdas_energia= row['Loss'] ), axis=1)

        # Add columns for "Vazio", "Cheio" or "Ponta"
        working_table[['Vazio', 'Cheio', 'Ponta', 'TAR']] = working_table.apply(lambda x: self.add_columns_energy_by_period (x, self.profile), axis=1)

        # Set the correct amounts for each period
        if (self.energy_cost_option == 'Simples'):
            vazio_energy = 0
            cheio_energy = vazio_energy + cheio_energy + ponta_energy
            ponta_energy = 0
        elif 'Bi-Horario' in self.energy_cost_option:
            cheio_energy = cheio_energy + ponta_energy
            ponta_energy = 0

        sum_profile_vazio = working_table['Vazio'].astype(float).sum()
        sum_profile_cheio = working_table['Cheio'].astype(float).sum()
        sum_profile_ponta = working_table['Ponta'].astype(float).sum()

        # Verify for zero division
        sum_profile_vazio = 1 if sum_profile_vazio == 0 else sum_profile_vazio
        sum_profile_cheio = 1 if sum_profile_cheio == 0 else sum_profile_cheio
        sum_profile_ponta = 1 if sum_profile_ponta == 0 else sum_profile_ponta
    
        # Calculate the energy amount
        working_table['Energy_vazio'] = working_table['Vazio'] * (vazio_energy / sum_profile_vazio)
        working_table['Energy_cheio'] = working_table['Cheio'] * (cheio_energy / sum_profile_cheio)
        working_table['Energy_ponta'] = working_table['Ponta'] * (ponta_energy / sum_profile_ponta)

        working_table['Cost_vazio'] = working_table['Energy_vazio'] * ( working_table['Price'] + working_table['TAR'] )
        working_table['Cost_cheio'] = working_table['Energy_cheio'] * ( working_table['Price'] + working_table['TAR'] )
        working_table['Cost_ponta'] = working_table['Energy_ponta'] * ( working_table['Price'] + working_table['TAR'] )

        # Sum the energy and cost for each period
        energy_vazio_sum =  working_table['Energy_vazio'].astype(float).sum()
        energy_cheio_sum =  working_table['Energy_cheio'].astype(float).sum()
        energy_ponta_sum =  working_table['Energy_ponta'].astype(float).sum()
        cost_vazio_sum   =  working_table['Cost_vazio'].astype(float).sum()
        cost_cheio_sum   =  working_table['Cost_cheio'].astype(float).sum()
        cost_ponta_sum   =  working_table['Cost_ponta'].astype(float).sum()

        # Calculate the average cost for each period
        vazio_avg_cost = 0 if energy_vazio_sum == 0 else cost_vazio_sum / energy_vazio_sum
        cheio_avg_cost = 0 if energy_cheio_sum == 0 else cost_cheio_sum / energy_cheio_sum
        ponta_avg_cost = 0 if energy_ponta_sum == 0 else cost_ponta_sum / energy_ponta_sum
        
        # Create a new column "Cost" with the sum of the three columns without modifying the original columns
        working_table['Cost'] = working_table['Cost_cheio'].fillna(0) + working_table['Cost_ponta'].fillna(0) + working_table['Cost_vazio'].fillna(0)
        
        # Drop the columns that are not needed to be returned
        col_subset = [col for col in ENERGY_PROFILES if col != self.profile]
        col_subset += ['Cost_vazio', 'Cost_cheio', 'Cost_ponta', 'Vazio', 'Cheio', 'Ponta']
        working_table.drop(columns= col_subset, inplace=True)
        
        # Reanme the columns 'Energy_vazio', 'Energy_cheio' and 'Energy_ponta'
        working_table.rename(columns={'Energy_vazio': 'Vazio', 'Energy_cheio': 'Cheio', 'Energy_ponta': 'Ponta', self.profile: 'Profile'}, inplace=True)
        
        # Create a new column "Energy" with the sum of the three columns without modifying the original columns
        working_table['Energy'] = working_table['Vazio'].fillna(0) + working_table['Cheio'].fillna(0) + working_table['Ponta'].fillna(0)
        
        recs = ''
        if records == 'csv':
            # Export the DataFrame to CSV
            recs = working_table.to_csv(index=True, date_format='%Y-%m-%d %H:%M', path_or_buf=None)
        elif records == 'json':
            # Export the DataFrame to JSON
            # Format the index     
            working_table.index = working_table.index.strftime('%Y-%m-%d %H:%M')
            # Convert the DataFrame to JSON
            recs = json.loads ( working_table.to_json( orient='table', index=True) )['data']       

        data_dict = {
            'Start_date' : start_date.strftime("%Y-%d-%m %H:%M"),
            'End_date' : end_date.strftime("%Y-%d-%m %H:%M"),
            'Total_Energy' : (vazio_energy+cheio_energy+ponta_energy),
            'Energy_Vazio' : energy_vazio_sum,
            'Energy_Cheio' : energy_cheio_sum, 
            'Energy_Ponta' : energy_ponta_sum,
            'Total_cost' : cost_vazio_sum + cost_cheio_sum + cost_ponta_sum,
            'Cost_Vazio' : cost_vazio_sum,
            'Cost_Cheio' : cost_cheio_sum,
            'Cost_Ponta' : cost_ponta_sum,
            'Vazio_avg_price_cost' : vazio_avg_cost,
            'Cheio_avg_price_cost' : cheio_avg_cost,
            'Ponta_avg_price_cost' : ponta_avg_cost,
            'recs' : recs  
        }
        return data_dict

    def calc_contagem (self, start_date, end_date, contagens):
        # Convert start_date and end_date to datetime objects
        start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d %H:%M')
        end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d %H:%M')

        closest_start_date = None
        closest_end_date = None
        min_start_diff = float('inf')
        min_end_diff = float('inf')

        for entry in contagens:
            current_date = datetime.datetime.strptime(entry['Data_da_Leitura'], '%Y-%m-%d %H:%M')

            start_diff = abs((current_date - start_date).total_seconds())
            end_diff = abs((current_date - end_date).total_seconds())

            if start_diff < min_start_diff:
                min_start_diff = start_diff
                closest_start_date = entry

            if end_diff < min_end_diff:
                min_end_diff = end_diff
                closest_end_date = entry

        # Calculate the difference for "Vazio", "Ponta", and "Cheias"
        differences = {
            'Vazio': float(closest_end_date['Vazio']) - float(closest_start_date['Vazio']),
            'Ponta': float(closest_end_date['Ponta']) - float(closest_start_date['Ponta']),
            'Cheias': float(closest_end_date['Cheias']) - float(closest_start_date['Cheias'])
        }

        return differences
    
    ##########
    def calc_master_table (self, profile_table, loss_profile_table, omie_table, master_file ):
        # Get the start and end date of the OMIE data we have
        start_date = omie_table.index.min() 
        end_date = omie_table.index.max()

        # Load existing master_file (if exist)
        try:
            master_table = pd.read_feather(master_file)
            # Convert the 'Date' column to datetime
            master_table['Date'] = pd.to_datetime(master_table['Date'])
            # Set the 'Date' column as the index
            master_table.set_index('Date', inplace=True)

            max_date = master_table.index.max()
            if end_date <= max_date:
                self.logger.info("No need to recalculate MASTER Price Table. Existing data is up to date.")
                return master_table
            
        except FileNotFoundError:
            self.logger.info ("MASTER Price File does NOT Exist....Creating new...")
    
        self.logger.info ("Calculate MASTER Price Table for the period: {} - {}".format(start_date, end_date))

        # Select the consumption profile data for the period
        master_table = profile_table.loc[start_date:end_date].copy ()

        ##### Calculate the OMIE Price for each row
        # Filter the OMIE data for the period
        omie_df = omie_table.loc[start_date:end_date]['Price_PT'].copy()
        
        # add -1 hour
        omie_df.index = omie_df.index - pd.Timedelta(hours=1) # 1

        # Resample omie_data to match the 15-min intervals of master_table and convert to kWh
        omie_data_resampled = omie_df.resample('15min').ffill() / 1000.0

        # Create "OMIE Price" column
        master_table['OMIE Price'] = omie_data_resampled.reindex(master_table.index, method='ffill')

        ##### Add the "Loss" column to the master_table
        # Filter the Loss data for the period
        loss_df = loss_profile_table.loc[start_date:end_date].copy()

        # Calculate "Total Loss" column
        loss_df['Total_Loss'] = ((1 + loss_df['BT']) * (1 + loss_df['MT']) * (1 + loss_df['AT']) * (1 + loss_df['ATRNT'])) - 1

        # Align the indexes and create the 'Loss' column in the 'Prices Table' DataFrame
        if (loss_df.index.is_unique == False):
            # If there's duplicates in the index, consider only the first and remove the others;
            loss_df = loss_df.reset_index().drop_duplicates(subset='Date', keep='first').set_index('Date')

        master_table['Loss'] = loss_df['Total_Loss'].reindex(master_table.index, method='ffill')

        # Prepare all the Data to Calculate the Cost for each Supplier given Energy = 1
        #####

        master_table[['TAR-Simples', 'Period-Simples-Cycle', 'Period-Simples']]                                     = master_table.index.to_series().apply(lambda date: pd.Series(self.get_price_tar(date, 'Simples') ))
        master_table[['TAR-Bi-Horario-Semanal', 'Period-Bi-Horario-Semanal-Cycle', 'Period-Bi-Horario-Semanal']]    = master_table.index.to_series().apply(lambda date: pd.Series(self.get_price_tar(date, 'Bi-Horario-Semanal') ) )
        master_table[['TAR-Bi-Horario-Diario', 'Period-Bi-Horario-Diario-Cycle','Period-Bi-Horario-Diario']]        = master_table.index.to_series().apply(lambda date: pd.Series(self.get_price_tar(date, 'Bi-Horario-Diario') ) )
        master_table[['TAR-Tri-Horario-Semanal', 'Period-Tri-Horario-Semanal-Cycle','Period-Tri-Horario-Semanal']]  = master_table.index.to_series().apply(lambda date: pd.Series(self.get_price_tar(date, 'Tri-Horario-Semanal') ) )
        master_table[['TAR-Tri-Horario-Diario', 'Period-Tri-Horario-Diario-Cycle','Period-Tri-Horario-Diario']]     = master_table.index.to_series().apply(lambda date: pd.Series(self.get_price_tar(date, 'Tri-Horario-Diario') ) )

        # Calculate the energy price for each row
        master_table['Energy-Cost-Coopernico-Base'] = master_table.apply(lambda row: self.calc_coopernico_price(row['OMIE Price']*1000, row.name, 0 if pd.isna(row['Loss']) else row['Loss'], 'coopernico-base' ), axis=1)
        master_table['Energy-Cost-Coopernico-Go']   = master_table.apply(lambda row: self.calc_coopernico_price(row['OMIE Price']*1000, row.name, 0 if pd.isna(row['Loss']) else row['Loss'], 'coopernico-go' ), axis=1)

        # Save data to Feather file
        master_table.reset_index().to_feather(master_file)

        return master_table

    def get_master_table_columns_from_options (self):
        # Get the TAR column of the Energy_cost_option set
        tar_column = {
            'Simples'            : 'TAR-Simples', 
            'Bi-Horario-Semanal' : 'TAR-Bi-Horario-Semanal', 
            'Bi-Horario-Diario'  : 'TAR-Bi-Horario-Diario',
            'Tri-Horario-Semanal': 'TAR-Tri-Horario-Semanal',
            'Tri-Horario-Diario' : 'TAR-Tri-Horario-Diario'
        }.get(self.energy_cost_option, None)

        # Get the TAR Period column of the Energy_cost_option set
        tar_period_column = {
            'Simples'            : 'Period-Simples-Cycle', 
            'Bi-Horario-Semanal' : 'Period-Bi-Horario-Semanal-Cycle', 
            'Bi-Horario-Diario'  : 'Period-Bi-Horario-Diario-Cycle',
            'Tri-Horario-Semanal': 'Period-Tri-Horario-Semanal-Cycle',
            'Tri-Horario-Diario' : 'Period-Tri-Horario-Diario-Cycle'
        }.get(self.energy_cost_option, None)

        # Get the Supplier Plan column of the energy_supplier
        supplier_column = {
            'coopernico-base'   : 'Energy-Cost-Coopernico-Base', 
            'coopernico-go'     : 'Energy-Cost-Coopernico-Go', 
        }.get(self.energy_supplier, None)

        return supplier_column, tar_column, tar_period_column

    def get_current_price (self, master_table, date, lagHour):
        # Get the Columns of Master Table for the Options on Self
        supplier_column, tar_column, tar_period_column = self.get_master_table_columns_from_options()

        # get the row closest to "date"
        date = pd.Timestamp (date)

        ## Get the row data from Master Table
        sub_data = master_table.loc[:date].iloc[-1]

        start_date, end_date = self.get_current_cycle_period(self.cycle_day)
        cycle_period = {
            'start': start_date.strftime('%Y-%m-%d %H:%M'), 
            'end': end_date.strftime('%Y-%m-%d %H:%M')
        }
        
        ret_value = {
            'Date' : sub_data.name,
            'Supplier_Price' : sub_data[supplier_column],
            'Cost' : sub_data[supplier_column] + sub_data[tar_column],
            'OMIE_Price' : sub_data['OMIE Price']*1000,
            'Tariff': self.energy_cost_option,
            'Supplier': self.energy_supplier,
            'Cycle Period' : cycle_period,
            'TAR': sub_data[tar_column],
            'TAR Period' : sub_data[tar_period_column]
        }
        return ret_value

    def get_energy_cost_between_dates(self, master_table, start_date, end_date, lagHour):
        # Using the "Master Table" Algo

        # Get the Columns of Master Table for the Options on Self
        supplier_column, tar_column, tar_period_column = self.get_master_table_columns_from_options()

        # Create a copy DataFrame between start_date and end_date
        sub_data = master_table.loc[start_date:end_date].copy()

        sub_data[['Vazio', 'Cheio', 'Ponta']] = sub_data.apply(lambda row: pd.Series([row[self.profile] if row[tar_period_column]['periodo'] == 'Vazio' else None,
                                                                                      row[self.profile] if row[tar_period_column]['periodo'] == 'Cheio' else None,
                                                                                      row[self.profile] if row[tar_period_column]['periodo'] == 'Ponta' else None]), axis=1)

        # Rename and Drop unecessary columns
        sub_data = sub_data.rename(columns={'OMIE Price': 'Price_PT'})
        sub_data['Price_PT'] = sub_data['Price_PT'] * 1000

        sub_data = sub_data.rename(columns={tar_period_column: 'TAR-Period',
                                            tar_column: 'TAR',
                                            self.profile : 'Energy',
                                            })


        columns_to_keep = ['Price_PT', 'Loss', 'TAR', 'TAR-Period', supplier_column, 'Energy', 'Vazio', 'Cheio', 'Ponta']
        sub_data = sub_data.drop(columns=sub_data.columns.difference(columns_to_keep))

        # Perform the processing od data on Master Table sub:
        # Calculate the Cost of Energy        
        sub_data['Cost'] = sub_data[supplier_column] + sub_data['TAR']
        

        # Resample to 1 hour period
        # sub_data = sub_data.resample('H').agg({'Price_PT': 'first',
        #                                        'TAR-Period' : 'first',
        #                                        'Loss': 'first', 
        #                                        'TAR': 'first', 
        #                                        supplier_column : 'first', 
        #                                        'Energy' : 'first',
        #                                        'Cost': 'first',
        #                                        'Vazio': 'first', 
        #                                        'Cheio': 'first', 
        #                                        'Ponta': 'first'
        #                                        })
        
        # Generate the JSON structures

        # Create a new DataFrame with only the columns you want in your JSON
        prices_df = sub_data[['Price_PT', 'Cost']].copy()
        prices_df['Energy'] = 1 

        # Create a new DataFrame with only the columns you want in your JSON
        records_df = sub_data[['Cheio', 'Vazio', 'Ponta', 'Energy', 'TAR']].copy()
        records_df.index = records_df.index.strftime('%Y-%m-%d %H:%M')

        return sub_data, prices_df, records_df