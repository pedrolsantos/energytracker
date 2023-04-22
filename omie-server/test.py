import datetime
from Calculations import EnergyCosts
from Ingestion import DataIngestion
import json

data_ingest = DataIngestion('OMIE_Data/')
energy_cost = EnergyCosts('Tri-Horario-Semanal', 2023)


# Get a specific value from the DataFrame
#print (omie_data.loc['2023-03-24 07:00:00']['Price_PT'])
#print ( get_price_from_date (omie_data, '2023-03-23 15:30', 'Price_PT'))

dfConsumo = data_ingest.load_ERedes_Consumption_data ('Consumos/Consumos_PT166184330_Mar23.xlsx')
# #print (dfConsumo.head (10))
# dfConsumo = energy_cost.add_energy_consumption_cost_column (dfConsumo, data_ingest.omie_data)

# print (dfConsumo)

# print(f'Daylight Saving Time starts: {energy_cost.dst_start}')
# print(f'Daylight Saving Time ends: {energy_cost.dst_end}')
# data = datetime.datetime.strptime('2023-03-23 18:30', '%Y-%m-%d %H:%M')

# tar = energy_cost.get_price_tar(data)


# data_ingest.download_OMIE_today_data()

# date = datetime.datetime.now() 
# if date < data_ingest.omie_data.index.max():
#     print ( {'error': 'No OMIE data found for today', 'Date':date, 'Max Date':data_ingest.omie_data.index.max() })

# omie_price = energy_cost.get_omie_price_for_date (data_ingest.omie_data, date)
# tar_price = energy_cost.get_price_tar (date)
# price = energy_cost.calc_energy_price (date, 1, data_ingest.omie_data )

# print ( {'date': date, 'net price': price, 'OMIE price': omie_price, 'TAR':tar_price })


# start_date = datetime.datetime.strptime('2023-03-01 00:00', '%Y-%m-%d %H:%M')
# end_date = datetime.datetime.strptime('2023-03-02 12:00', '%Y-%m-%d %H:%M')
# period_energy = energy_cost.get_energy_cost_between_dates (dfConsumo, data_ingest.omie_data, start_date, end_date)
# print (period_energy)


    
start_date = datetime.datetime.strptime("2023-03-01-00:00", "%Y-%m-%d-%H:%M")
end_date = datetime.datetime.strptime("2023-03-28-02:30", "%Y-%m-%d-%H:%M")
vazio = 496
cheio = 154
ponta = 69

# response = energy_cost.get_profile_estimation_manual (data_ingest.profile_data, data_ingest.omie_data, start_date, end_date, vazio, cheio, ponta)

count = energy_cost.count_15min_periods_today()

print ( 'end' )
