import math
import datetime
from datetime import time
import pytz
import pandas as pd
import json
from logger_config import setup_logger

#######   TEMPLATE DE CUSTOS DE ENERGIA   #######
Energy_Time_Cycle = {
    'Simples': [
        {'dst':1, 'dias':0, 'start': '00:00', 'end': '23:59', 'tar': -0.0958, 'periodo':'Cheio'}, # Seg a Sexta 
        {'dst':1, 'dias':1, 'start': '00:00', 'end': '23:59', 'tar': -0.0958, 'periodo':'Cheio'}, # Sábado
        {'dst':1, 'dias':2, 'start': '00:00', 'end': '23:59', 'tar': -0.0958, 'periodo':'Cheio'}, # Domingo

        {'dst':0, 'dias':0, 'start': '00:00', 'end': '23:59', 'tar': -0.0958, 'periodo':'Cheio'}, # Seg a Sexta 
        {'dst':0, 'dias':1, 'start': '00:00', 'end': '23:59', 'tar': -0.0958, 'periodo':'Cheio'}, # Sábado
        {'dst':0, 'dias':2, 'start': '00:00', 'end': '23:59', 'tar': -0.0958, 'periodo':'Cheio'}, # Domingo
    ],
    'Bi-Horario-Semanal': [
        {'dst':1, 'dias':0, 'start': '00:00', 'end': '06:59', 'tar': -0.1185, 'periodo':'Vazio'}, # Seg a Sexta em Vazio no Verão
        {'dst':1, 'dias':0, 'start': '07:00', 'end': '23:59', 'tar': -0.0842, 'periodo':'Cheio'}, # Seg a Sexta em Fora de Vazio no Verão
        {'dst':1, 'dias':1, 'start': '00:00', 'end': '08:59', 'tar': -0.1185, 'periodo':'Vazio'}, # Sábado em Vazio no Verão
        {'dst':1, 'dias':1, 'start': '09:00', 'end': '13:59', 'tar': -0.0842, 'periodo':'Cheio'}, # Sábado em Fora Vazio no Verão
        {'dst':1, 'dias':1, 'start': '14:00', 'end': '19:59', 'tar': -0.1185, 'periodo':'Vazio'}, # Sábado em Vazio no Verão
        {'dst':1, 'dias':1, 'start': '20:00', 'end': '21:59', 'tar': -0.0842, 'periodo':'Cheio'}, # Sábado em Fora Vazio no Verão
        {'dst':1, 'dias':1, 'start': '22:00', 'end': '23:59', 'tar': -0.1185, 'periodo':'Vazio'}, # Sábado em Vazio no Verão
        {'dst':1, 'dias':2, 'start': '00:00', 'end': '23:59', 'tar': -0.1185, 'periodo':'Vazio'}, # Domingo em Vazio no Verão

        {'dst':0, 'dias':0, 'start': '00:00', 'end': '06:59', 'tar': -0.1185, 'periodo':'Vazio'}, # Seg a Sexta em Vazio no Inverno
        {'dst':0, 'dias':0, 'start': '07:00', 'end': '23:59', 'tar': -0.0842, 'periodo':'Cheio'}, # Seg a Sexta em Fora de Vazio no Inverno
        {'dst':0, 'dias':1, 'start': '00:00', 'end': '09:29', 'tar': -0.1185, 'periodo':'Vazio'}, # Sábado em Vazio no Inverno
        {'dst':0, 'dias':1, 'start': '09:30', 'end': '12:59', 'tar': -0.0842, 'periodo':'Cheio'}, # Sábado em Fora Vazio no Inverno
        {'dst':0, 'dias':1, 'start': '13:00', 'end': '18:29', 'tar': -0.1185, 'periodo':'Vazio'}, # Sábado em Vazio no Inverno
        {'dst':0, 'dias':1, 'start': '18:30', 'end': '21:59', 'tar': -0.0842, 'periodo':'Cheio'}, # Sábado em Fora Vazio no Inverno
        {'dst':0, 'dias':1, 'start': '22:00', 'end': '23:59', 'tar': -0.1185, 'periodo':'Vazio'}, # Sábado em Vazio no Inverno
        {'dst':0, 'dias':2, 'start': '00:00', 'end': '23:59', 'tar': -0.1185, 'periodo':'Vazio'}, # Domingo em Vazio no Inverno
    ],
    'Bi-Horario-Diario': [
        {'dst':1, 'dias':0, 'start': '00:00', 'end': '07:59', 'tar': -0.1185, 'periodo':'Vazio'}, # Seg a Sexta em Vazio no Verão
        {'dst':1, 'dias':0, 'start': '08:00', 'end': '21:59', 'tar': -0.0842, 'periodo':'Cheio'}, # Seg a Sexta em Fora de Vazio no Verão
        {'dst':1, 'dias':0, 'start': '22:00', 'end': '23:59', 'tar': -0.1185, 'periodo':'Vazio'}, # Seg a Sexta em Vazio no Verão

        {'dst':1, 'dias':1, 'start': '00:00', 'end': '07:59', 'tar': -0.1185, 'periodo':'Vazio'}, # Sábado em Vazio no Verão
        {'dst':1, 'dias':1, 'start': '08:00', 'end': '21:59', 'tar': -0.0842, 'periodo':'Cheio'}, # Sábado em Fora de Vazio no Verão
        {'dst':1, 'dias':1, 'start': '22:00', 'end': '23:59', 'tar': -0.1185, 'periodo':'Vazio'}, # Sábado em Vazio no Verão

        {'dst':1, 'dias':2, 'start': '00:00', 'end': '07:59', 'tar': -0.1185, 'periodo':'Vazio'}, # Domingo em Vazio no Verão
        {'dst':1, 'dias':2, 'start': '08:00', 'end': '21:59', 'tar': -0.0842, 'periodo':'Cheio'}, # Domingo em Fora de Vazio no Verão
        {'dst':1, 'dias':2, 'start': '22:00', 'end': '23:59', 'tar': -0.1185, 'periodo':'Vazio'}, # Domingo em Vazio no Verão

        {'dst':0, 'dias':0, 'start': '00:00', 'end': '07:59', 'tar': -0.1185, 'periodo':'Vazio'}, # Seg a Sexta em Vazio no Inverno
        {'dst':0, 'dias':0, 'start': '08:00', 'end': '21:59', 'tar': -0.0842, 'periodo':'Cheio'}, # Seg a Sexta em Fora de Vazio no Inverno
        {'dst':0, 'dias':0, 'start': '22:00', 'end': '23:59', 'tar': -0.1185, 'periodo':'Vazio'}, # Seg a Sexta em Vazio no Inverno

        {'dst':0, 'dias':1, 'start': '00:00', 'end': '07:59', 'tar': -0.1185, 'periodo':'Vazio'}, # Sábado em Vazio no Inverno
        {'dst':0, 'dias':1, 'start': '08:00', 'end': '21:59', 'tar': -0.0842, 'periodo':'Cheio'}, # Sábado em Fora de Vazio no Inverno
        {'dst':0, 'dias':1, 'start': '22:00', 'end': '23:59', 'tar': -0.1185, 'periodo':'Vazio'}, # Sábado em Vazio no Inverno

        {'dst':0, 'dias':2, 'start': '00:00', 'end': '07:59', 'tar': -0.1185, 'periodo':'Vazio'}, # Domingo em Vazio no Inverno
        {'dst':0, 'dias':2, 'start': '08:00', 'end': '21:59', 'tar': -0.0842, 'periodo':'Cheio'}, # Domingo em Fora de Vazio no Inverno
        {'dst':0, 'dias':2, 'start': '22:00', 'end': '23:59', 'tar': -0.1185, 'periodo':'Vazio'}, # Domingo em Vazio no Inverno
    ],
    'Tri-Horario-Semanal': [
        {'dst':1, 'dias':0, 'start': '00:00', 'end': '06:59', 'tar': -0.1185, 'periodo':'Vazio'}, # Seg a Sexta em Vazio no Verão
        {'dst':1, 'dias':0, 'start': '07:00', 'end': '09:14', 'tar': -0.1069, 'periodo':'Cheio'}, # Seg a Sexta em Cheio no Verão
        {'dst':1, 'dias':0, 'start': '09:15', 'end': '12:14', 'tar': -0.0018, 'periodo':'Ponta'}, # Seg a Sexta em Ponta no Verão
        {'dst':1, 'dias':0, 'start': '12:15', 'end': '23:59', 'tar': -0.1069, 'periodo':'Cheio'}, # Seg a Sexta em Cheio no Verão
        {'dst':1, 'dias':1, 'start': '00:00', 'end': '08:59', 'tar': -0.1185, 'periodo':'Vazio'}, # Sábado em Vazio no Verão
        {'dst':1, 'dias':1, 'start': '09:00', 'end': '13:59', 'tar': -0.1069, 'periodo':'Cheio'}, # Sábado em Cheio no Verão
        {'dst':1, 'dias':1, 'start': '14:00', 'end': '19:59', 'tar': -0.1185, 'periodo':'Vazio'}, # Sábado em Vazio no Verão
        {'dst':1, 'dias':1, 'start': '20:00', 'end': '21:59', 'tar': -0.1069, 'periodo':'Cheio'}, # Sábado em Cheio no Verão
        {'dst':1, 'dias':1, 'start': '22:00', 'end': '23:59', 'tar': -0.1185, 'periodo':'Vazio'}, # Sábado em Vazio no Verão
        {'dst':1, 'dias':2, 'start': '00:00', 'end': '23:59', 'tar': -0.1185, 'periodo':'Vazio'}, # Domingo em Vazio no Verão

        {'dst':0, 'dias':0, 'start': '00:00', 'end': '06:59', 'tar': -0.1185, 'periodo':'Vazio'}, # Seg a Sexta em Vazio no Inverno
        {'dst':0, 'dias':0, 'start': '07:00', 'end': '09:29', 'tar': -0.1069, 'periodo':'Cheio'}, # Seg a Sexta em Cheio no Inverno
        {'dst':0, 'dias':0, 'start': '09:30', 'end': '11:59', 'tar': -0.0018, 'periodo':'Ponta'}, # Seg a Sexta em Ponta no Inverno
        {'dst':0, 'dias':0, 'start': '12:00', 'end': '18:29', 'tar': -0.1069, 'periodo':'Cheio'}, # Seg a Sexta em Cheio no Inverno
        {'dst':0, 'dias':0, 'start': '18:30', 'end': '20:59', 'tar': -0.0018, 'periodo':'Ponta'}, # Seg a Sexta em Ponta no Inverno
        {'dst':0, 'dias':0, 'start': '21:00', 'end': '23:59', 'tar': -0.1069, 'periodo':'Cheio'}, # Seg a Sexta em Cheio no Inverno
        {'dst':0, 'dias':1, 'start': '00:00', 'end': '09:29', 'tar': -0.1185, 'periodo':'Vazio'}, # Sábado em Vazio no Inverno
        {'dst':0, 'dias':1, 'start': '09:30', 'end': '12:59', 'tar': -0.1069, 'periodo':'Cheio'}, # Sábado em Cheio no Inverno
        {'dst':0, 'dias':1, 'start': '13:00', 'end': '18:29', 'tar': -0.1185, 'periodo':'Vazio'}, # Sábado em Vazio no Inverno
        {'dst':0, 'dias':1, 'start': '18:30', 'end': '21:59', 'tar': -0.1069, 'periodo':'Cheio'}, # Sábado em Cheio no Inverno
        {'dst':0, 'dias':1, 'start': '22:00', 'end': '23:59', 'tar': -0.1185, 'periodo':'Vazio'}, # Sábado em Vazio no Inverno
        {'dst':0, 'dias':2, 'start': '00:00', 'end': '23:59', 'tar': -0.1185, 'periodo':'Vazio'}, # Domingo em Vazio no Inverno
    ],
    'Tri-Horario-Diario': [
        {'dst':1, 'dias':0, 'start': '00:00', 'end': '07:59', 'tar': -0.1185, 'periodo':'Vazio'}, # Seg a Sexta em Vazio no Verão
        {'dst':1, 'dias':0, 'start': '08:00', 'end': '10:29', 'tar': -0.1069, 'periodo':'Cheio'}, # Seg a Sexta em Cheio no Verão
        {'dst':1, 'dias':0, 'start': '10:30', 'end': '12:59', 'tar': -0.0018, 'periodo':'Ponta'}, # Seg a Sexta em Ponta no Verão
        {'dst':1, 'dias':0, 'start': '13:00', 'end': '19:29', 'tar': -0.1069, 'periodo':'Cheio'}, # Seg a Sexta em Cheio no Verão
        {'dst':1, 'dias':0, 'start': '19:30', 'end': '20:59', 'tar': -0.0018, 'periodo':'Ponta'}, # Seg a Sexta em Ponta no Verão
        {'dst':1, 'dias':0, 'start': '21:00', 'end': '21:59', 'tar': -0.1069, 'periodo':'Cheio'}, # Seg a Sexta em Cheio no Verão
        {'dst':1, 'dias':0, 'start': '22:00', 'end': '23:59', 'tar': -0.1185, 'periodo':'Vazio'}, # Seg a Sexta em Vazio no Verão

        {'dst':1, 'dias':1, 'start': '00:00', 'end': '07:59', 'tar': -0.1185, 'periodo':'Vazio'}, # Sabado em Vazio no Verão
        {'dst':1, 'dias':1, 'start': '08:00', 'end': '10:29', 'tar': -0.1069, 'periodo':'Cheio'}, # Sabado em Cheio no Verão
        {'dst':1, 'dias':1, 'start': '10:30', 'end': '12:59', 'tar': -0.0018, 'periodo':'Ponta'}, # Sabado em Ponta no Verão
        {'dst':1, 'dias':1, 'start': '13:00', 'end': '19:29', 'tar': -0.1069, 'periodo':'Cheio'}, # Sabado em Cheio no Verão
        {'dst':1, 'dias':1, 'start': '19:30', 'end': '20:59', 'tar': -0.0018, 'periodo':'Ponta'}, # Sabado em Ponta no Verão
        {'dst':1, 'dias':1, 'start': '21:00', 'end': '21:59', 'tar': -0.1069, 'periodo':'Cheio'}, # Sabado em Cheio no Verão
        {'dst':1, 'dias':1, 'start': '22:00', 'end': '23:59', 'tar': -0.1185, 'periodo':'Vazio'}, # Sabado em Vazio no Verão

        {'dst':1, 'dias':2, 'start': '00:00', 'end': '07:59', 'tar': -0.1185, 'periodo':'Vazio'}, # Domingo em Vazio no Verão
        {'dst':1, 'dias':2, 'start': '08:00', 'end': '10:29', 'tar': -0.1069, 'periodo':'Cheio'}, # Domingo em Cheio no Verão
        {'dst':1, 'dias':2, 'start': '10:30', 'end': '12:59', 'tar': -0.0018, 'periodo':'Ponta'}, # Domingo em Ponta no Verão
        {'dst':1, 'dias':2, 'start': '13:00', 'end': '19:29', 'tar': -0.1069, 'periodo':'Cheio'}, # Domingo em Cheio no Verão
        {'dst':1, 'dias':2, 'start': '19:30', 'end': '20:59', 'tar': -0.0018, 'periodo':'Ponta'}, # Domingo em Ponta no Verão
        {'dst':1, 'dias':2, 'start': '21:00', 'end': '21:59', 'tar': -0.1069, 'periodo':'Cheio'}, # Domingo em Cheio no Verão
        {'dst':1, 'dias':2, 'start': '22:00', 'end': '23:59', 'tar': -0.1185, 'periodo':'Vazio'}, # Domingo em Vazio no Verão

        {'dst':0, 'dias':0, 'start': '00:00', 'end': '07:59', 'tar': -0.1185, 'periodo':'Vazio'}, # Seg a Sexta em Vazio no Inverno
        {'dst':0, 'dias':0, 'start': '08:00', 'end': '08:59', 'tar': -0.1069, 'periodo':'Cheio'}, # Seg a Sexta em Cheio no Inverno
        {'dst':0, 'dias':0, 'start': '09:00', 'end': '10:29', 'tar': -0.0018, 'periodo':'Ponta'}, # Seg a Sexta em Ponta no Inverno
        {'dst':0, 'dias':0, 'start': '10:30', 'end': '17:59', 'tar': -0.1069, 'periodo':'Cheio'}, # Seg a Sexta em Cheio no Inverno
        {'dst':0, 'dias':0, 'start': '18:00', 'end': '20:29', 'tar': -0.0018, 'periodo':'Ponta'}, # Seg a Sexta em Ponta no Inverno
        {'dst':0, 'dias':0, 'start': '20:30', 'end': '21:59', 'tar': -0.1069, 'periodo':'Cheio'}, # Seg a Sexta em Cheio no Inverno
        {'dst':0, 'dias':0, 'start': '22:00', 'end': '23:59', 'tar': -0.1185, 'periodo':'Vazio'}, # Seg a Sexta em Vazio no Inverno

        {'dst':0, 'dias':1, 'start': '00:00', 'end': '07:59', 'tar': -0.1185, 'periodo':'Vazio'}, # Sabado em Vazio no Inverno
        {'dst':0, 'dias':1, 'start': '08:00', 'end': '08:59', 'tar': -0.1069, 'periodo':'Cheio'}, # Sabado em Cheio no Inverno
        {'dst':0, 'dias':1, 'start': '09:00', 'end': '10:29', 'tar': -0.0018, 'periodo':'Ponta'}, # Sabado em Ponta no Inverno
        {'dst':0, 'dias':1, 'start': '10:30', 'end': '17:59', 'tar': -0.1069, 'periodo':'Cheio'}, # Sabado em Cheio no Inverno
        {'dst':0, 'dias':1, 'start': '18:00', 'end': '20:29', 'tar': -0.0018, 'periodo':'Ponta'}, # Sabado em Ponta no Inverno
        {'dst':0, 'dias':1, 'start': '20:30', 'end': '21:59', 'tar': -0.1069, 'periodo':'Cheio'}, # Sabado em Cheio no Inverno
        {'dst':0, 'dias':1, 'start': '22:00', 'end': '23:59', 'tar': -0.1185, 'periodo':'Vazio'}, # Sabado em Vazio no Inverno

        {'dst':0, 'dias':2, 'start': '00:00', 'end': '07:59', 'tar': -0.1185, 'periodo':'Vazio'}, # Domingo em Vazio no Inverno
        {'dst':0, 'dias':2, 'start': '08:00', 'end': '08:59', 'tar': -0.1069, 'periodo':'Cheio'}, # Domingo em Cheio no Inverno
        {'dst':0, 'dias':2, 'start': '09:00', 'end': '10:29', 'tar': -0.0018, 'periodo':'Ponta'}, # Domingo em Ponta no Inverno
        {'dst':0, 'dias':2, 'start': '10:30', 'end': '17:59', 'tar': -0.1069, 'periodo':'Cheio'}, # Domingo em Cheio no Inverno
        {'dst':0, 'dias':2, 'start': '18:00', 'end': '20:29', 'tar': -0.0018, 'periodo':'Ponta'}, # Domingo em Ponta no Inverno
        {'dst':0, 'dias':2, 'start': '20:30', 'end': '21:59', 'tar': -0.1069, 'periodo':'Cheio'}, # Domingo em Cheio no Inverno
        {'dst':0, 'dias':2, 'start': '22:00', 'end': '23:59', 'tar': -0.1185, 'periodo':'Vazio'}, # Domingo em Vazio no Inverno

    ]
}

###### FORNECEDORES DE ENERGIA ######
Energy_Suppliers = {'coopernico-base', 'coopernico-go', 'luzboa-spot'}

###### PERFIS DE CONSUMO ######
Energy_Profiles = ['BTN-A', 'BTN-B', 'BTN-C', 'IP', 'mP', 
                   'UPAC-A-CV-Consumo', 'UPAC-B-CV-Consumo', 'UPAC-C-CV-Consumo', 
                   'UPAC-A-Consumo', 'UPAC-B-Consumo', 'UPAC-C-Consumo']

### CLASSES ###
class EnergyCosts():
    logger = None
    dst_start = None
    dst_end =  None
    energy_cost_option = None
    energy_supplier = None
    cycle_day = None
    profile = None
    master_prices_table = None
    luzboa_prices = None


    def __init__(self, option, year, supplier, cycle_day=1, profile= 'BTN-C', timezone = 'Europe/Lisbon'):
        self.logger = setup_logger('EnergyCosts')
        dst_start, dst_end = self.get_dst_dates (year, timezone)
        self.dst_start = dst_start
        self.dst_end = dst_end
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
        tz = pytz.timezone(timezone)

        # Last Sunday of March
        dst_start = datetime.datetime(year, 3, 31, 2, 0, tzinfo=tz)
        while dst_start.weekday() != 6:  # 6 represents Sunday
            dst_start -= datetime.timedelta(days=1)

        # Last Sunday of November
        dst_end = datetime.datetime(year, 11, 30, 2, 0, tzinfo=tz)
        while dst_end.weekday() != 6:  # 6 represents Sunday
            dst_end -= datetime.timedelta(days=1)

        # Remove timezone information
        dst_start = dst_start.replace(tzinfo=None)
        dst_end = dst_end.replace(tzinfo=None)
        return dst_start, dst_end

    def is_daylight_savings(self, date):
        return self.dst_start <= date < self.dst_end

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
        if now.day < cycle_day:
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

    def get_price_tar (self, str_date):
        date = pd.to_datetime(str_date)        
        hour_minute = date.time()
        date_dst = 1 if self.is_daylight_savings(date) else 0
        date_category = self.get_day_category(date)

        tar = 0 # Default value
        for period in Energy_Time_Cycle[self.energy_cost_option]:
            if (period['dst'] == date_dst):
                if (period['dias'] == date_category):
                    start_time = datetime.datetime.strptime(period['start'], '%H:%M').time()
                    end_time = datetime.datetime.strptime(period['end'], '%H:%M').time()
                    if start_time <= hour_minute <= end_time:
                        tar = period['tar']
                        #print (f"Date: {date} - Tar: {tar} - Row: {period}")
                        break

        return tar, period

    def get_loss_profile_for_date (self, loss_profile_data, date):
        target_time = pd.to_datetime(date)
        loss = 0

        # Get the last available date equal to or less than the target_time
        subset_profile = loss_profile_data.loc[loss_profile_data.index <= target_time]
        last_valid_index = subset_profile.last_valid_index()
        
        if last_valid_index is None:
            self.logger.error("get_loss_profile_for_date: No available data before or on target_time:" + str(target_time))
            return 0, {}

        last_valid_data = loss_profile_data.loc[last_valid_index]

        # Profile formula:  ((1+BT) x (1+MT) x (1+AT) x (1+ATRNT)) -1
        loss = ((1 + last_valid_data['BT']) * (1 + last_valid_data['MT']) * (1 + last_valid_data['AT']) * (1 + last_valid_data['ATRNT'])) - 1

        loss_date = last_valid_data.name

        ret_data = { 
            'Loss Date': loss_date.strftime('%Y-%m-%d %H:%M'), 
            'Loss': loss
        }
        return loss, ret_data

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
        
    def calc_coopernico_price (self, omie_price, custos_gestao=0.004, margem=0.01, fator_perdas_energia=0.15658358):
        # Coopernico Formula : Preço Energia (€/kWh) = (PM + CGS + k + GO) x (1+FP)
        # PM = Preço OMIE / 1000
        # CGS = Custo de Gestão do Sistema
        # k = Margem de Comercialização
        # FP = Fator de Potência
        # https://www.coopernico.pt/pt/energia/precos-energia/precos-energia-2021

        if (self.energy_supplier == 'coopernico-base'):
            cost_GO = 0
        elif (self.energy_supplier == 'coopernico-go'):
            cost_GO = 0.01

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

    def calc_energy_price (self, date, energy_consumption, omie_data, loss_profile_data, lagHour=1, price_field='Price_PT', divisionFactor=1):
        # Check if the energy consumption is NaN
        if (math.isnan(energy_consumption)):
            return 0, {}
        
        # Get OMIE price for the given time for PT
        omie_price, omie_ret_data  = self.get_omie_price_for_date (omie_data, date, price_field, lagHour=lagHour)

        # Get the price tar
        price_tar, period = self.get_price_tar (date)

        # Get loss for data:
        loss, loss_data = self.get_loss_profile_for_date (loss_profile_data, date)

        price = 0
        start_date, end_date = self.get_current_cycle_period(self.cycle_day)
        cycle_period = {'start': start_date, 'end': end_date}

        if ( 'coopernico' in self.energy_supplier):
            price = self.calc_coopernico_price (omie_price, fator_perdas_energia=loss)
        elif (self.energy_supplier == 'luzboa-spot'):
            if (self.luzboa_prices is not None):
                if (self.energy_cost_option == 'Simples'):
                    price = self.luzboa_prices['simples']
                elif 'Bi-Horario' in self.energy_cost_option:
                    price = self.luzboa_prices['avgVazio'] if (period['periodo'] == 'Vazio') else self.luzboa_prices['avgForaVazio']
                elif 'Tri-Horario' in self.energy_cost_option:
                    price = self.luzboa_prices['avgVazio'] if (period['periodo'] == 'Vazio') else self.luzboa_prices['avgCheio'] if (period['periodo'] == 'Cheio') else self.luzboa_prices['avgPonta']

        # Calculate the energy cost
        cost = (energy_consumption / divisionFactor) * (price + price_tar)

        ret_data = { 
                'Date': date.strftime('%Y-%m-%d %H:%M'),
                'Price': price,
                'Cost:': cost,
                'Energy Divisor': divisionFactor,
                'Energy': energy_consumption,
                'Supplier': self.energy_supplier,
                'Cycle Period': cycle_period,
                'TAR': price_tar,
                'TAR Data': period,
                'Loss Data': loss_data, 
                'OMIE Data': omie_ret_data,
        }

        return cost, ret_data

    def calc_master_table (self, profile_table, loss_profile_table, omie_table, omie_column='Price_PT'  ):
        # Get the start and end date of the Omie data we have
        start_date = omie_table.index.min() 
        end_date = omie_table.index.max() 

        # Select the consumption profile data for the period
        master_table = profile_table.loc[start_date:end_date].copy ()

        ##### Calculate the OMIE Price for each row
        # Filter the OMIE data for the period
        omie_df = omie_table.loc[start_date:end_date][omie_column].copy()
        
        # add -1 hour
        omie_df.index = omie_df.index - pd.Timedelta(hours=1)

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
        master_table['Loss'] = loss_df['Total_Loss'].reindex(master_table.index, method='ffill')

        self.logger.info ("Calculate MASTER Price Table for the period: {} - {}".format(start_date, end_date))
        return master_table

    def get_luzboa_average_prices (self, start_date, end_date, lagHour=1):
        # Convert the dates to datetime
        start_date = pd.to_datetime(start_date)
        end_date = pd.to_datetime(end_date)

        # Filter the master_table for the period
        working_table = self.master_prices_table.loc[start_date:end_date].copy()

        # Calculate the price for each row
        working_table['Price'] = working_table.apply(lambda row: self.calc_luzboa_price (row['OMIE Price'] , fator_perdas_energia= row['Loss']), axis=1)

        # Add columns for "Vazio", "Cheio" or "Ponta"
        working_table[['Vazio', 'Cheio', 'Ponta', 'TAR']] = working_table.apply(lambda x: self.add_columns_energy_by_period (x, self.profile), axis=1)

        # Calc the net price column
        working_table['PriceNET'] = working_table['Price'] + working_table['TAR']

        price_simples = working_table['PriceNET'].resample('H').mean().mean() 
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
        price_tar, period = self.get_price_tar ( date )

        if period['periodo'] =='Vazio':
            return pd.Series( [energy, None, None, price_tar], index=['Vazio', 'Cheio', 'Ponta', 'TAR'] )
        elif period['periodo'] =='Cheio':
            return pd.Series( [None, energy, None, price_tar], index=['Vazio', 'Cheio', 'Ponta', 'TAR'] )
        elif period['periodo'] =='Ponta':
            return pd.Series( [None, None, energy, price_tar], index=['Vazio', 'Cheio', 'Ponta', 'TAR'] )
        
        # If the period is not defined, return None
        return pd.Series( [None, None, None, None], index=['Vazio', 'Cheio', 'Ponta', 'TAR'] )

    def add_energy_consumption_cost_column (self, energy_data, omie_data, loss_profile_data, energy_column= 'Energy', divisionFactor=1, lagHour=1):
        # Calculate the energy price for each row
        energy_data['Cost'] = energy_data.apply(lambda row: self.calc_energy_price (row.name, row[energy_column], omie_data, loss_profile_data, lagHour, 'Price_PT', divisionFactor)[0], axis=1)
        if divisionFactor > 1:
            energy_data[energy_column] = energy_data[energy_column] / divisionFactor
        
        # Add the columns 'Vazio', 'Cheio' and 'Ponta' with the energy consumption for each period
        energy_data[['Vazio', 'Cheio', 'Ponta', 'TAR']] = energy_data.apply(self.add_columns_energy_by_period, axis=1)
        return energy_data
    
    def get_energy_cost_between_dates(self, energy_data, omie_data, loss_profile_data, start_date, end_date, lagHour):
        filtered_energy_data = None 
        if energy_data is None:
            # add lagHour to the start_date and end_date
            start_date = start_date + datetime.timedelta(hours=lagHour)
            end_date = end_date + datetime.timedelta(hours=lagHour)

            # Create a new DataFrame with the same index as omie_data between start_date and end_date
            filtered_energy_data = omie_data.loc[start_date:end_date].copy()

            # Take 1 hour to the index datetime becasue of CEST timeframe of OMIE data
            filtered_energy_data.index = filtered_energy_data.index + pd.Timedelta(hours=-lagHour)

            # Add the 'Energy' column with a value of 1 for all rows
            filtered_energy_data['Energy'] = 1
        else:
            # Filter the energy data
            filtered_energy_data = energy_data.loc[start_date:end_date].copy()
        
        if (self.energy_supplier == 'luzboa-spot'):
            self.update_luzboa_cache_prices()

        # Calculate the energy price for each row
        filtered_energy_data['Cost'] = filtered_energy_data.apply(lambda row: self.calc_energy_price( (row.name + datetime.timedelta(minutes=30)).to_pydatetime().replace(tzinfo=None), row['Energy'], omie_data, loss_profile_data, lagHour, 'Price_PT')[0], axis=1)

        return filtered_energy_data

    def get_profile_estimation(self, profile_data, omie_data, loss_profile_data, start_date, end_date, total_energy, records='none', lagHour=1):
        total_energy= float (total_energy)
        sum_profile_period = profile_data.loc[start_date:end_date][self.profile].sum()
        energy_profile = profile_data.loc[start_date:end_date][self.profile] * (total_energy / sum_profile_period)

        # Convert the Series to a DataFrame and set the column name
        energy_profile_df = energy_profile.to_frame()
        energy_profile_df.columns = ['Energy']

        energy_profile_df = self.add_energy_consumption_cost_column (energy_profile_df, omie_data, loss_profile_data, lagHour=lagHour)

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
    
    def get_profile_by_period(self, profile_data, start_date, end_date):
        # Create a new DataFrame with the same index as profile_data between start_date and end_date
        subset_profile = profile_data.loc[start_date:end_date][self.profile].copy()
        subset_profile = subset_profile.to_frame()
        subset_profile.columns = [self.profile]

        # Add the columns 'Vazio', 'Cheio' and 'Ponta' with the energy consumption for each period
        subset_profile[['Vazio', 'Cheio', 'Ponta', 'TAR']] = subset_profile.apply(lambda x: self.add_columns_energy_by_period (x, self.profile), axis=1)

        # Drop the columns 'BTN C'
        subset_profile.drop(columns=[self.profile], inplace=True)
        
        # Create a new column "Energy" with the sum of the three columns without modifying the original columns
        subset_profile['Energy'] = subset_profile['Vazio'].fillna(0) + subset_profile['Cheio'].fillna(0) + subset_profile['Ponta'].fillna(0)

        # Format the index     
        subset_profile.index = subset_profile.index.strftime('%Y-%m-%d %H:%M')
        
        # Convert the DataFrame to JSON
        recs = json.loads ( subset_profile.to_json( orient='table', index=True) )['data']

        return recs

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
        start_date = pd.to_datetime(start_date)
        end_date = pd.to_datetime(end_date)

        # Filter the master_table for the period
        working_table = self.master_prices_table.loc[start_date:end_date].copy()

        # Calculate the price for each row
        working_table['Price'] = working_table.apply(lambda row: self.calc_coopernico_price (row['OMIE Price']*1000 , fator_perdas_energia= row['Loss'] ), axis=1)

        # Add columns for "Vazio", "Cheio" or "Ponta"
        working_table[['Vazio', 'Cheio', 'Ponta', 'TAR']] = working_table.apply(lambda x: self.add_columns_energy_by_period (x, self.profile), axis=1)

        # Set the correct amounts for each period
        if (self.energy_cost_option == 'Simples'):
            vazio_energy = vazio_energy + cheio_energy + ponta_energy
            cheio_energy = 0
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
        col_subset = [col for col in Energy_Profiles if col != self.profile]
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
