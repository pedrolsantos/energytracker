# pip install flask
# pip install flask-cors
# pip install pytz

# Rest API
import os
import uuid
import pandas as pd
import datetime 
from datetime import timedelta, date
import pytz 
import json
from flask import Flask, request, jsonify, send_from_directory, after_this_request
from flask_cors import CORS

from logger_config import setup_logger, setBasePath

from Calculations import EnergyCosts, Energy_Time_Cycle, Energy_Suppliers
from Ingestion import DataIngestion

###################  FOLDERS  ############################
currentDir = os.path.dirname(os.path.abspath(__file__))
if os.path.exists('OMIE_Data/') and os.path.isdir('OMIE_Data/'):
    BASE_PATH = currentDir
else:
    BASE_PATH = os.path.join(currentDir, "..")

BASE_PATH = os.path.join(currentDir, '..' )

OMIE_PATH = os.path.join(BASE_PATH, 'OMIE_Data/')
CONSUMOS_PATH = os.path.join(BASE_PATH, 'Consumos/')
EREDES_PATH = os.path.join(BASE_PATH, 'ERedes_profiles/')

###################  FLASK INIT  ############################
app = Flask(__name__)
CORS(app)

#####  HELPER FUNCTIONS  #####
def generate_dates_since_last_max(toDate):
    # Get the maximum index value from the data_ingest.omie_data dataframe
    max_date = data_ingest.omie_data.index.max()

    # Define a timedelta of one day
    one_day = timedelta(days=1)

    # Initialize a list to hold the filenames
    filenames = []

    # Loop over the range of dates from max_date to today
    date = max_date.date() + one_day
    while date <= toDate.date():
        filename = date
        filenames.append(filename)
        date += one_day

    return filenames

def check_and_download_OMIE ():
    ret = False
    date = datetime.datetime.now() + timedelta(days=1)
    currentHour = date.hour 
    date = date.replace(hour=0, minute=0)
    if date > data_ingest.omie_data.index.max(): # new day - start checking procedure
        dates = generate_dates_since_last_max(date)

        if (len(dates) == 1) and (currentHour >= 13): # if one day has passed and it is after 13:00 - try to download new data 
            for req_date in dates:
                data_ingest.download_OMIE_data(req_date)
            data_ingest.load_OMIE_data(data_ingest.omie_folder)
            app.logger.info ('OMIE today data updated successfully')
            ret = True
        elif (len(dates) > 1): # if more than one day has passed - download all new data available
            for req_date in dates:
                data_ingest.download_OMIE_data(req_date)
            data_ingest.load_OMIE_data(data_ingest.omie_folder)
            app.logger.info ('OMIE data updated successfully')
            ret = True
        else:
            app.logger.info ('OMIE data check skipped')
    else:
        app.logger.info ('OMIE data is up to date')
    
    return ret

def send_file_and_delete(file_path, filename, folder=CONSUMOS_PATH):
    if os.path.exists(file_path):
        @after_this_request
        def cleanup(response):
            try:
                os.remove(file_path)
            except Exception as error:
                app.logger.error("Error removing file: %s", error)
            return response

        return send_from_directory(directory=folder, path=filename, as_attachment=True)

def initialize_app():
    setBasePath(BASE_PATH)
    app.logger = setup_logger('ServiceAPI')
    
    data_ingest.download_OMIE_data()
    
    energy_cost = EnergyCosts('Tri-Horario-Semanal', 2023, 'luzboa-spot', 1 )
    data_ingest.luzBoa_data = energy_cost.calc_luzboa_price_table(data_ingest.omie_data, data_ingest.profile_loss_data)


############## API REST ################################# 
@app.route('/refreshOMIE', methods=['GET'])
def refresh_omie_data():
    # test with:
    # curl -X GET "http://127.0.0.1:5000/refreshOMIE?try_download=True"
    app.logger.info ("Service: refresh_omie_data")

    try_download = request.args.get('try_download')
    if try_download or try_download == 'True':
        app.logger.info ('refresh_omie_data: Downloading OMIE data for today')
        data_ingest.download_OMIE_data()
    
    data_ingest.load_OMIE_data(data_ingest.omie_folder)

    start_date = data_ingest.omie_data.index.min()
    end_date = data_ingest.omie_data.index.max()

    app.logger.info ("Service Done - refreshOMIE")
    return jsonify({'message': 'OMIE data updated successfully',
                    'start date': start_date,
                    'end date':end_date}), 200

@app.route('/downloadOMIE', methods=['GET'])
def download_omie_data():
    # test with:
    # curl -X GET "http://127.0.0.1:5000/DownloadOMIE?date=2021-02-23"
    app.logger.info ("Service: download_omie_data")

    str_date = request.args.get('date')
    if not str_date:
        app.logger.error('download_omie_data: Invalid request data')
        return jsonify({'error': 'Invalid request data'}), 400

    date = datetime.datetime.strptime(str_date, '%Y-%m-%d')
    status = data_ingest.download_OMIE_data(date)
    
    if status['saved'] == True:
        status['message'] = 'OMIE data updated successfully'
    else:
        status['message'] = 'OMIE data not updated'    
    
    app.logger.info ("Service Done - downloadOMIE")
    return jsonify( status ), 200

@app.route('/uploadEnergyFile', methods=['POST'])
def upload_energy_file():
    # Test with:
    # curl -X POST -F "file=@./ERedes_PT166184330_Fev23.xlsx"  "http://127.0.0.1:5000/uploadEnergyFile?supplier=coopernico-base&tariff=Tri-Horario-Semanal&year=2023&cycle_day=1&format=json&provider=E-Redes"
    app.logger.info ("Service: upload_energy_file")

    str_supplier = request.args.get('supplier')
    if (str_supplier not in Energy_Suppliers):
        app.logger.error('upload_energy_file: Invalid Energy Supplier')
        return jsonify({'error': 'Invalid Energy Supplier'}), 400

    str_tarifario = request.args.get('tariff')
    if (str_tarifario not in Energy_Time_Cycle):
        app.logger.error('upload_energy_file: Invalid tariff')
        return jsonify({'error': 'Invalid tariff'}), 400
    
    str_year = request.args.get('year')
    if (not str_year) or (not str_year.isdigit()) or (int(str_year) < 1900) or (int(str_year) > 2100) :
        app.logger.error('upload_energy_file: Invalid year')
        return jsonify({'error': 'Invalid year'}), 400

    str_exportformat = request.args.get('format')
    if not str_exportformat or str_exportformat not in {'xls', 'csv', 'json'}:
        app.logger.error('upload_energy_file: Invalid export format')
        return jsonify({'error': 'Invalid export format'}), 400

    str_provider = request.args.get('provider')
    if not str_provider or str_provider not in {'E-Redes', 'EoT' }:
        app.logger.error('upload_energy_file: Invalid provider')
        return jsonify({'error': 'Invalid provider'}), 400

    arg_cycle_day = request.args.get('cycle_day')
    if (not arg_cycle_day) or (not arg_cycle_day.isdigit()) or (int(arg_cycle_day) < 0) or (int(arg_cycle_day) > 31) :
        app.logger.error ("upload_energy_file: Invalid cycle_day" + arg_cycle_day)
        return jsonify({'error': 'Invalid cycle_day'}), 400
    cycle_day = int(arg_cycle_day)

    arg_lagHour = request.args.get('lagHour')
    laghour = 1
    if (arg_lagHour)  :
        laghour = int(arg_lagHour)

    # check if the post request has the file part
    if 'file' not in request.files:
        app.logger.error('upload_energy_file: No file part')
        return jsonify({'error': 'No file part'}), 400

    file = request.files['file']

    if file.filename == '':
        app.logger.error('upload_energy_file: No file selected')
        return jsonify({'error': 'No file selected'}), 400

    if file:
        # Get the file extension
        file_extension = os.path.splitext(file.filename)[1]

        # Generate a unique identifier (GUID) and maintain the file extension
        new_filename = f"{uuid.uuid4()}{file_extension}"

        filepath = os.path.join(CONSUMOS_PATH, new_filename)
        file.save(filepath)

    # Initialize the EnergyCosts class
    energy_cost = EnergyCosts(str_tarifario, int(str_year) , str_supplier, cycle_day)
    energy_cost.setLuzboaPrices (data_ingest.luzBoa_data)
    
    divisionFactor = 1
    if (str_provider == 'E-Redes'):
        # Load the file into a DataFrame
        dfConsumo = data_ingest.load_ERedes_Consumption_data (filepath)
        divisionFactor = 4 
    elif (str_provider == 'EoT'):
        # Load the file into a DataFrame
        dfConsumo = data_ingest.load_EOT_Consumption_data (filepath)
        divisionFactor = 1 

    # Add the energy cost column
    dfConsumo = energy_cost.add_energy_consumption_cost_column (dfConsumo, data_ingest.omie_data, data_ingest.profile_loss_data, 'Energy', divisionFactor, lagHour=laghour)

    # Save the file
    if str_exportformat == 'xls':
        filename = 'Consumos_' + datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S') + '.xlsx'
        file_path = os.path.join(CONSUMOS_PATH, filename)
        #dfConsumo['Date'] = dfConsumo['Date'].apply(lambda x: x.isoformat())
        dfConsumo.index = dfConsumo.index.strftime('%Y-%m-%d %H:%M')
        dfConsumo.reset_index(inplace=True)
        dfConsumo.to_excel( file_path, index=True)
        send_file_and_delete(file_path, filename, './Consumos')

    elif str_exportformat == 'csv':
        filename = 'Consumos_' + datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S') + '.csv'
        file_path = os.path.join(CONSUMOS_PATH, filename)
        dfConsumo.index = dfConsumo.index.strftime('%Y-%m-%d %H:%M')
        dfConsumo.reset_index(inplace=True)
        #dfConsumo['Date'] = dfConsumo['Date'].apply(lambda x: x.isoformat())
        dfConsumo.to_csv( file_path, index=False)
        send_file_and_delete(file_path, filename, './Consumos')

    elif str_exportformat == 'json':
        # Format the datetime index using strftime
        dfConsumo.index = dfConsumo.index.strftime('%Y-%m-%d %H:%M')
        # Convert the DataFrame to JSON
        records = json.loads ( dfConsumo.to_json( orient='table', index=True) )['data']
    
    app.logger.info ("Service Done - uploadEnergyFile")
    return records, 200

@app.route('/getPriceForDate', methods=['GET'])
def get_price_for_date():
    # test with:
    # curl -X GET "http://127.0.0.1:5000/getPriceForDate?supplier=Coopernico&tariff=Tri-Horario-Semanal&year=2023&date=2023-03-23-12:00&cycle_day=1"
    app.logger.info ("Service: getPriceForDate")

    str_supplier = request.args.get('supplier')
    if (str_supplier not in Energy_Suppliers):
        app.logger.error ("get_price_for_date: Invalid Energy Supplier")
        return jsonify({'error': 'Invalid Energy Supplier'}), 400

    str_tarifario = request.args.get('tariff')
    if (str_tarifario not in Energy_Time_Cycle):
        app.logger.error ("get_price_for_date: Invalid tariff")
        return jsonify({'error': 'Invalid tariff'}), 400
    
    str_date = request.args.get('date')
    if not str_date:
        app.logger.error ("get_price_for_date: Invalid request data")
        return jsonify({'error': 'Invalid request data'}), 400
    
    str_year = request.args.get('year')
    if (not str_year) or (not str_year.isdigit()) or (int(str_year) < 1900) or (int(str_year) > 2100) :
        app.logger.error ("get_price_for_date: Invalid year")
        return jsonify({'error': 'Invalid year'}), 400

    arg_cycle_day = request.args.get('cycle_day')
    if (not arg_cycle_day) or (not arg_cycle_day.isdigit()) or (int(arg_cycle_day) < 0) or (int(arg_cycle_day) > 31) :
        app.logger.error ("get_price_for_date: Invalid cycle_day" + arg_cycle_day)
        return jsonify({'error': 'Invalid cycle_day'}), 400
    cycle_day = int(arg_cycle_day)

    arg_lagHour = request.args.get('lagHour')
    laghour = 1
    if (arg_lagHour)  :
        laghour = int(arg_lagHour)

    # Initialize the EnergyCosts class
    energy_cost = EnergyCosts(str_tarifario, int(str_year), str_supplier, cycle_day )
    energy_cost.setLuzboaPrices (data_ingest.luzBoa_data)

    # Check if OMIE data is updated
    ret = check_and_download_OMIE ()
    if (ret):
        # Recalculate the LuzBoa price table
        data_ingest.luzBoa_data = energy_cost.calc_luzboa_price_table (data_ingest.omie_data, data_ingest.profile_loss_data)

    date = datetime.datetime.strptime(str_date, '%Y-%m-%d-%H:%M')
    omie_price, omie_ret_data  = energy_cost.get_omie_price_for_date (data_ingest.omie_data, date, lagHour=laghour)
    tar_price, tar_period = energy_cost.get_price_tar (date)
    price, price_data = energy_cost.calc_energy_price (date, 1, data_ingest.omie_data,  data_ingest.profile_loss_data, lagHour=laghour )

    app.logger.info ("Service Done - getPriceForDate")
    return jsonify({'date': date.strftime('%Y-%m-%d %H:%M'), 
                    'net price': price, 
                    'OMIE price': omie_price, 
                    'OMIE Data': omie_ret_data,
                    'Tariff': energy_cost.energy_cost_option,
                    'TAR':tar_price,
                    'TAR Period': tar_period }), 200

@app.route('/getCurrentPrice', methods=['GET'])
def get_current_price():
    # test with:
    # curl -X GET "http://127.0.0.1:5000/getCurrentPrice?supplier=coopernico-base&tariff=Tri-Horario-Semanal&year=2023&cycle_day=1"
    app.logger.info ("Service: getCurrentPrice")

    str_supplier = request.args.get('supplier')
    if (str_supplier not in Energy_Suppliers):
        app.logger.error ("get_current_price: Invalid Energy Supplier")
        return jsonify({'error': 'Invalid Energy Supplier'}), 400

    str_tarifario = request.args.get('tariff')
    if (str_tarifario not in Energy_Time_Cycle):
        app.logger.error ("get_current_price: Invalid tariff")
        return jsonify({'error': 'Invalid tariff'}), 400
    
    str_year = request.args.get('year')
    if (not str_year) or (not str_year.isdigit()) or (int(str_year) < 1900) or (int(str_year) > 2100) :
        app.logger.error ("get_current_price: Invalid year")
        return jsonify({'error': 'Invalid year'}), 400

    arg_cycle_day = request.args.get('cycle_day')
    if (not arg_cycle_day) or (not arg_cycle_day.isdigit()) or (int(arg_cycle_day) < 0) or (int(arg_cycle_day) > 31) :
        app.logger.error ("get_current_price: Invalid cycle_day" + str(arg_cycle_day))
        return jsonify({'error': 'Invalid cycle_day'}), 400
    cycle_day = int(arg_cycle_day)

    arg_lagHour = request.args.get('lagHour')
    laghour = 1
    if (arg_lagHour)  :
        laghour = int(arg_lagHour)

    # Initialize the EnergyCosts class
    energy_cost = EnergyCosts(str_tarifario, int(str_year), str_supplier, cycle_day )
    energy_cost.setLuzboaPrices (data_ingest.luzBoa_data)

    # Check if OMIE data is updated
    ret = check_and_download_OMIE ()
    if (ret):
        # Recalculate the LuzBoa price table
        data_ingest.luzBoa_data = energy_cost.calc_luzboa_price_table (data_ingest.omie_data, data_ingest.profile_loss_data)
        energy_cost.setLuzboaPrices (data_ingest.luzBoa_data)

    date = datetime.datetime.now() 
    if date > data_ingest.omie_data.index.max():
        app.logger.error ("No OMIE data found for today")
        return jsonify({'error': 'No OMIE data found for today', 'Date':date }), 404
    
    omie_price, omie_ret_data = energy_cost.get_omie_price_for_date (data_ingest.omie_data, date, lagHour=laghour )
    tar_price, tar_period = energy_cost.get_price_tar (date)
    price, price_data = energy_cost.calc_energy_price (date, 1, data_ingest.omie_data, data_ingest.profile_loss_data, lagHour=laghour )

    price_data['Cycle Period']['start'] = price_data['Cycle Period']['start'].strftime('%Y-%m-%d %H:%M')
    price_data['Cycle Period']['end'] = price_data['Cycle Period']['end'].strftime('%Y-%m-%d %H:%M')

    app.logger.info ("Service Done - getCurrentPrice")
    return jsonify({'date': date.strftime('%Y-%m-%d %H:%M'), 
                    'net price': price, 
                    'OMIE price': omie_price, 
                    'OMIE Data': omie_ret_data,
                    'Tariff': energy_cost.energy_cost_option,
                    'Supplier': energy_cost.energy_supplier,
                    'Cycle Period': price_data['Cycle Period'],
                    'TAR':tar_price,
                    'TAR Period': tar_period }), 200

@app.route('/getOMIEPricesForPeriod', methods=['GET'])
def getOMIEPricesForPeriod ():
    # test with:
    # curl -X GET "http://127.0.0.1:5000/getOMIEPricesForPeriod?supplier=coopernico-base&tariff=Simples&year=2023&cycle_day=1&supplier=coopernico-base&start_date=2023-02-23-00:00&end_date=2023-02-24-00:00"
    app.logger.info ("Service: getOMIEPricesForPeriod")

    str_supplier = request.args.get('supplier')
    if (str_supplier not in Energy_Suppliers):
        app.logger.error ("getOMIEPricesForPeriod: Invalid Energy Supplier")
        return jsonify({'error': 'Invalid Energy Supplier'}), 400

    str_tarifario = request.args.get('tariff')
    if (str_tarifario not in Energy_Time_Cycle):
        app.logger.error ("getOMIEPricesForPeriod: Invalid tariff")
        return jsonify({'error': 'Invalid tariff'}), 400
    
    str_year = request.args.get('year')
    if (not str_year) or (not str_year.isdigit()) or (int(str_year) < 1900) or (int(str_year) > 2100) :
        app.logger.error ("getOMIEPricesForPeriod: Invalid year")
        return jsonify({'error': 'Invalid year'}), 400
    
    arg_cycle_day = request.args.get('cycle_day')
    if (not arg_cycle_day) or (not arg_cycle_day.isdigit()) or (int(arg_cycle_day) < 0) or (int(arg_cycle_day) > 31) :
        app.logger.error ("getOMIEPricesForPeriod: Invalid cycle_day" + arg_cycle_day)
        return jsonify({'error': 'Invalid cycle_day'}), 400
    cycle_day = int(arg_cycle_day)    

    # Initialize the EnergyCosts class
    energy_cost = EnergyCosts(str_tarifario, int(str_year), str_supplier, cycle_day )
    energy_cost.setLuzboaPrices (data_ingest.luzBoa_data)

    # Check if OMIE data is updated
    ret = check_and_download_OMIE ()
    if (ret):
        # Recalculate the LuzBoa price table
        data_ingest.luzBoa_data = energy_cost.calc_luzboa_price_table (data_ingest.omie_data, data_ingest.profile_loss_data)
        energy_cost.setLuzboaPrices (data_ingest.luzBoa_data)

    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')

    if not start_date or not end_date:
        app.logger.error ("Invalid request data")
        return jsonify({'error': 'Invalid request data'}), 400

    try:
        start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d-%H:%M')
        end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d-%H:%M')
    except ValueError:
        app.logger.error ("Invalid date format")
        return jsonify({'error': 'Invalid date format'}), 400
    
    if start_date > end_date:
        app.logger.error ("Invalid date range")
        return jsonify({'error': 'Invalid date range'}), 400
    
    if start_date < data_ingest.omie_data.index.min(): 
        app.logger.error ("Invalid date range on OMIE data")
        return jsonify({'error': 'Invalid date range on OMIE data'}), 400
    
    arg_lagHour = request.args.get('lagHour')
    laghour = 1
    if (arg_lagHour)  :
        laghour = int(arg_lagHour)

    prices_for_period = energy_cost.get_energy_cost_between_dates(None, data_ingest.omie_data, data_ingest.profile_loss_data, start_date,  end_date, laghour)

    if prices_for_period.empty:
        app.logger.error ("No OMIE data found for the given period")
        return jsonify({'error': 'No OMIE data found for the given period'}), 404

    prices_for_period.reset_index(inplace=True)
    prices_json = prices_for_period.to_json(orient='records', date_format='iso')
    prices_dict = json.loads(prices_json)

    records = energy_cost.get_profile_by_period(data_ingest.profile_data, start_date, end_date)
    
    app.logger.info ("Service Done - getOMIEPricesForPeriod")
    return jsonify({
        'start_date' : start_date.strftime("%Y-%d-%m %H:%M"),
        'end_date' : end_date.strftime("%Y-%d-%m %H:%M"),
        'prices' : prices_dict,
        'profile' : records
    }), 200

@app.route('/getEstimationProfile', methods=['GET'])
def estimate_profile_cost():
    # test with:
    # curl -X GET "http://127.0.0.1:5000/getEstimationProfile?tariff=Tri-Horario-Semanal&year=2023&cycle_day=1&supplier=coopernico-base&start_date=2023-02-23-00:00&end_date=2023-02-24-00:00&total_energy=655.32"
    app.logger.info ("Service: getEstimationProfile")

    str_supplier = request.args.get('supplier')
    if (str_supplier not in Energy_Suppliers):
        app.logger.error ("estimate_profile_cost: Invalid Energy Supplier")
        return jsonify({'error': 'Invalid Energy Supplier'}), 400

    str_tarifario = request.args.get('tariff')
    if (str_tarifario not in Energy_Time_Cycle):
        app.logger.error ("estimate_profile_cost: Invalid tariff")
        return jsonify({'error': 'Invalid tariff'}), 400
    
    str_year = request.args.get('year')
    if (not str_year) or (not str_year.isdigit()) or (int(str_year) < 1900) or (int(str_year) > 2100) :
        app.logger.error ("estimate_profile_cost: Invalid year")
        return jsonify({'error': 'Invalid year'}), 400

    arg_cycle_day = request.args.get('cycle_day')
    if (not arg_cycle_day) or (not arg_cycle_day.isdigit()) or (int(arg_cycle_day) < 0) or (int(arg_cycle_day) > 31) :
        app.logger.error ("estimate_profile_cost: Invalid cycle_day" + arg_cycle_day)
        return jsonify({'error': 'Invalid cycle_day'}), 400
    cycle_day = int(arg_cycle_day)

    # Initialize the EnergyCosts class
    energy_cost = EnergyCosts(str_tarifario, int(str_year), str_supplier, cycle_day )
    energy_cost.setLuzboaPrices (data_ingest.luzBoa_data)

    # Check if OMIE data is updated
    ret = check_and_download_OMIE ()
    if (ret):
        # Recalculate the LuzBoa price table
        data_ingest.luzBoa_data = energy_cost.calc_luzboa_price_table (data_ingest.omie_data, data_ingest.profile_loss_data)
        energy_cost.setLuzboaPrices (data_ingest.luzBoa_data)

    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')

    if not start_date or not end_date:
        app.logger.error ("Invalid request data")
        return jsonify({'error': 'Invalid request data'}), 400

    try:
        start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d-%H:%M')
        end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d-%H:%M')
    except ValueError:
        app.logger.error ("Invalid date format")
        return jsonify({'error': 'Invalid date format'}), 400
    
    if start_date > end_date:
        app.logger.error ("Invalid date range")
        return jsonify({'error': 'Invalid date range'}), 400
    
    if start_date < data_ingest.omie_data.index.min() or end_date > data_ingest.omie_data.index.max():
        app.logger.error ("Invalid date range on OMIE data")
        return jsonify({'error': 'Invalid date range on OMIE data'}), 400    

    total_energy = request.args.get('total_energy')
    if (not total_energy)  :
        app.logger.error ("Invalid total_energy")
        return jsonify({'error': 'Invalid total_energy'}), 400

    arg_records = request.args.get('records')
    records = 'none'
    if (arg_records)  :
        records = arg_records        

    response = energy_cost.get_profile_estimation (data_ingest.profile_data, data_ingest.omie_data, data_ingest.profile_loss_data, start_date, end_date, total_energy, records)
    
    app.logger.info("Service Done: getEstimationProfile")
    return jsonify({
        'Start_date' : response['Start_date'],
        'End_date' : response['End_date'],
        'Total_energy' : response['Total_Energy'],
        'Energy_Vazio': response['Energy_Vazio'],
        'Energy_Cheio' : response['Energy_Cheio'],
        'Energy_Ponta' : response['Energy_Ponta'],
        'Total_Cost' : response['Total_cost'],
        'Cost_Vazio' : response['Cost_Vazio'],
        'Cost_Cheio' : response['Cost_Cheio'],
        'Cost_Ponta' : response['Cost_Ponta'],
        'Vazio_avg_price_cost' : response['Vazio_avg_price_cost'],
        'Cheio_avg_price_cost' : response['Cheio_avg_price_cost'],
        'Ponta_avg_price_cost' : response['Ponta_avg_price_cost'],
        'records': response['recs']
    }), 200
    
@app.route('/getEstimationProfileManual', methods=['GET'])
def estimate_profile_cost_manual():
    # test with:
    # curl -X GET "http://127.0.0.1:5000/getEstimationProfile?tariff=Tri-Horario-Semanal&year=2023&start_date=2023-02-23-00:00&end_date=2023-02-24-00:00&total_vazio=155.32&total_cheio=155.32&total_ponta=155.32&cycle_day=1&supplier=coopernico-base"
    app.logger.info ("Service: getEstimationProfileManual")

    str_supplier = request.args.get('supplier')
    if (str_supplier not in Energy_Suppliers):
        app.logger.error("estimate_profile_cost_manual: Invalid Energy Supplier")
        return jsonify({'error': 'Invalid Energy Supplier'}), 400

    str_tarifario = request.args.get('tariff')
    if (str_tarifario not in Energy_Time_Cycle):
        app.logger.error("estimate_profile_cost_manual: Invalid tariff")
        return jsonify({'error': 'Invalid tariff'}), 400
    
    str_year = request.args.get('year')
    if (not str_year) or (not str_year.isdigit()) or (int(str_year) < 1900) or (int(str_year) > 2100) :
        app.logger.error("estimate_profile_cost_manual: Invalid year")
        return jsonify({'error': 'Invalid year'}), 400
    
    arg_cycle_day = request.args.get('cycle_day')
    if (not arg_cycle_day) or (not arg_cycle_day.isdigit()) or (int(arg_cycle_day) < 0) or (int(arg_cycle_day) > 31) :
        app.logger.error ("estimate_profile_cost_manual: Invalid cycle_day" + arg_cycle_day)
        return jsonify({'error': 'Invalid cycle_day'}), 400
    cycle_day = int(arg_cycle_day)    

    # Initialize the EnergyCosts class
    energy_cost = EnergyCosts(str_tarifario, int(str_year), str_supplier, cycle_day )
    energy_cost.setLuzboaPrices (data_ingest.luzBoa_data)

    # Check if OMIE data is updated
    ret = check_and_download_OMIE ()
    if (ret):
        # Recalculate the LuzBoa price table
        data_ingest.luzBoa_data = energy_cost.calc_luzboa_price_table (data_ingest.omie_data, data_ingest.profile_loss_data)
        energy_cost.setLuzboaPrices (data_ingest.luzBoa_data)

    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')

    if not start_date or not end_date:
        app.logger.error("Invalid request data")
        return jsonify({'error': 'Invalid request data'}), 400

    try:
        start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d-%H:%M')
        end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d-%H:%M')
    except ValueError:
        app.logger.error("Invalid date format")
        return jsonify({'error': 'Invalid date format'}), 400
    
    if start_date > end_date:
        app.logger.error("Invalid date range")
        return jsonify({'error': 'Invalid date range'}), 400
    
    if start_date < data_ingest.omie_data.index.min() or end_date > data_ingest.omie_data.index.max():
        app.logger.error("Invalid date range on OMIE data")
        return jsonify({'error': 'Invalid date range on OMIE data'}), 400

    total_vazio = request.args.get('total_vazio')
    if (not total_vazio):
        app.logger.error("Invalid total_vazio")
        return jsonify({'error': 'Invalid total_vazio'}), 400
    
    total_cheio = request.args.get('total_cheio')
    if (not total_cheio):
        app.logger.error("Invalid total_cheio")
        return jsonify({'error': 'Invalid total_cheio'}), 400
    
    total_ponta = request.args.get('total_ponta')
    if (not total_ponta):
        app.logger.error("Invalid total_ponta")
        return jsonify({'error': 'Invalid total_ponta'}), 400
    
    arg_records = request.args.get('records')
    records = 'none'
    if (arg_records)  :
        records = arg_records

    arg_lagHour = request.args.get('lagHour')
    laghour = 1
    if (arg_lagHour):
        laghour = int(arg_lagHour)
    
    response = energy_cost.get_profile_estimation_manual (data_ingest.profile_data, data_ingest.omie_data, data_ingest.profile_loss_data, start_date, end_date, total_vazio, total_cheio, total_ponta, laghour, records)
    
    app.logger.info("Service Done: getEstimationProfileManual")
    return jsonify({
        'Start_date' : response['Start_date'],
        'End_date' : response['End_date'],
        'Total_energy' : response['Total_Energy'],
        'Energy_Vazio': response['Energy_Vazio'],
        'Energy_Cheio' : response['Energy_Cheio'],
        'Energy_Ponta' : response['Energy_Ponta'],
        'Total_Cost' : response['Total_cost'],
        'Cost_Vazio' : response['Cost_Vazio'],
        'Cost_Cheio' : response['Cost_Cheio'],
        'Cost_Ponta' : response['Cost_Ponta'],
        'Vazio_avg_price_cost' : response['Vazio_avg_price_cost'],
        'Cheio_avg_price_cost' : response['Cheio_avg_price_cost'],
        'Ponta_avg_price_cost' : response['Ponta_avg_price_cost'],
        'records': response['recs']
    }), 200

@app.route('/getEOTData', methods=['GET'])
def get_eot_data ():
    # test with:
    # curl -X GET "http://127.0.0.1:5000/getEOTData?tariff=Tri-Horario-Semanal&year=2023?key=XXXXXXXXX&cycle_day=1&supplier=coopernico-base"
    app.logger.info ("Start Service: getEOTData")

    str_supplier = request.args.get('supplier')
    if (str_supplier not in Energy_Suppliers):
        app.logger.error("get_eot_data: Invalid Energy Supplier")
        return jsonify({'error': 'Invalid Energy Supplier'}), 400

    str_tarifario = request.args.get('tariff')
    if (str_tarifario not in Energy_Time_Cycle):
        app.logger.error("get_eot_data: Invalid tariff")
        return jsonify({'error': 'Invalid tariff'}), 400
    
    str_year = request.args.get('year')
    if (not str_year) or (not str_year.isdigit()) or (int(str_year) < 1900) or (int(str_year) > 2100) :
        app.logger.error("get_eot_data: Invalid year")
        return jsonify({'error': 'Invalid year'}), 400

    str_eotKey = request.args.get('key')
    if (not str_eotKey) or (len(str_eotKey) != 16) or (str_eotKey == 'XXXXXXXXX'):
        app.logger.error("get_eot_data: Invalid EOT Key")
        return jsonify({'error': 'Invalid Key'}), 400

    arg_cycle_day = request.args.get('cycle_day')
    if (not arg_cycle_day) or (not arg_cycle_day.isdigit()) or (int(arg_cycle_day) < 0) or (int(arg_cycle_day) > 31) :
        app.logger.error ("get_eot_data: Invalid cycle_day")
        return jsonify({'error': 'Invalid cycle_day'}), 400
    cycle_day = int(arg_cycle_day)

    arg_lagHour = request.args.get('lagHour')
    laghour = 1
    if (arg_lagHour)  :
        laghour = int(arg_lagHour)

    # Call the function to get the EOT data
    response = data_ingest.get_EOT_Data (api_key=str_eotKey, results=1, channel='iap_diff')
    if (response is None) or (response == -1):
        app.logger.error("Error calling EOT API")
        return jsonify({'error': 'Invalid Call to EOT'}), 400

    date_power = response['channels'][0]['feeds'][0]['created_at']
    power = response['channels'][0]['feeds'][0]['value']

    # Initialize the EnergyCosts class
    energy_cost = EnergyCosts(str_tarifario, int(str_year) , str_supplier, cycle_day)
    energy_cost.setLuzboaPrices (data_ingest.luzBoa_data)

    # find the number of 15 min periods since midnight
    count_periods = energy_cost.count_15min_periods_today()

    response = data_ingest.get_EOT_Data (api_key=str_eotKey, results=count_periods, channel='tiae')
    if (response is None) or (response == -1):
        app.logger.error("Error calling EOT API")
        return jsonify({'error': 'Invalid Call to EOT'}), 400
    
    # Transform the response into a dataframe
    df = pd.DataFrame (response['channels'][0]['feeds'])

    # Rename the columns
    df = df.rename(columns={'dif': 'Energy'})    
    
    # Convert the timestamp to a timezone-aware datetime object using UTC
    df['created_at'] = pd.to_datetime(df['created_at'], format="%Y-%m-%d %H:%M:%S %Z") #.dt.tz_localize('UTC')
    
    # Define your local timezone
    local_tz = pytz.timezone('Europe/Lisbon') 
    
    # Convert the datetime objects to your local timezone
    df['created_at'] = df['created_at'].dt.tz_convert(local_tz)
    
    # Calculate the time difference with midnight
    df['time_diff'] = df['created_at'].apply(lambda x: abs(x - x.replace(hour=0, minute=0, second=0, microsecond=0)))

    # Find the row with the minimum time difference
    closest_element = df.loc[df['time_diff'].idxmin()]

    # Filter rows between the closest element and now
    filtered_df =  df[(df['created_at'] >= closest_element['created_at'] ) ].copy ()  

    # Set the "Date" column as the index    
    filtered_df['created_at'] = filtered_df['created_at'].dt.tz_localize(None)
    filtered_df.set_index('created_at', inplace=True)
    filtered_df = filtered_df.sort_index()

    # Add the energy cost column  
    dfConsumo = energy_cost.add_energy_consumption_cost_column (filtered_df, data_ingest.omie_data, data_ingest.profile_loss_data, 'Energy', lagHour=laghour)

    # Sum values in the filtered rows
    total_energy = filtered_df['Energy'].sum()
    total_cost = filtered_df['Cost'].sum()

    # Get the current energy consumption
    current_energy_consumption_power = response['channels'][0]['feeds'][0]['total']

    app.logger.info("End Service: getEOTData")
    return jsonify({'date': date_power, 
                    'power': power,  
                    'energy_last': current_energy_consumption_power,
                    'energy_total': total_energy,
                    'total_cost': total_cost,
                    }), 200    

@app.route('/getProfilePeriod', methods=['GET'])
def getProfilePeriod():
    # test with:
    # curl -X GET "http://127.0.0.1:5000/getProfilePeriod?tariff=Tri-Horario-Semanal&year=2023&start_date=2023-02-23-00:00&end_date=2023-02-24-00:00&cycle_day=1&supplier=coopernico-base"
    app.logger.info ("Service: getProfilePeriod")

    str_supplier = request.args.get('supplier')
    if (str_supplier not in Energy_Suppliers):
        return jsonify({'error': 'Invalid Energy Supplier'}), 400

    str_tarifario = request.args.get('tariff')
    if (str_tarifario not in Energy_Time_Cycle):
        app.logger.error("getProfilePeriod: Invalid tariff: " + str_tarifario)
        return jsonify({'error': 'Invalid tariff'}), 400
    
    str_year = request.args.get('year')
    if (not str_year) or (not str_year.isdigit()) or (int(str_year) < 1900) or (int(str_year) > 2100) :
        app.logger.error("getProfilePeriod: Invalid year: " + str_year)
        return jsonify({'error': 'Invalid year'}), 400
    
    arg_cycle_day = request.args.get('cycle_day')
    if (not arg_cycle_day) or (not arg_cycle_day.isdigit()) or (int(arg_cycle_day) < 0) or (int(arg_cycle_day) > 31) :
        app.logger.error ("getProfilePeriod: Invalid cycle_day" + arg_cycle_day)
        return jsonify({'error': 'Invalid cycle_day'}), 400
    cycle_day = int(arg_cycle_day)    

    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')

    if not start_date or not end_date:
        app.logger.error("getProfilePeriod: Invalid request data")
        return jsonify({'error': 'Invalid request data'}), 400

    try:
        start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d-%H:%M')
        end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d-%H:%M')
    except ValueError:
        app.logger.error("getProfilePeriod: Invalid date format")
        return jsonify({'error': 'Invalid date format'}), 400
    
    if start_date > end_date:
        app.logger.error("getProfilePeriod: Invalid date range")
        return jsonify({'error': 'Invalid date range'}), 400

    # Initialize the EnergyCosts class
    energy_cost = EnergyCosts(str_tarifario, int(str_year) , str_supplier, cycle_day)
    energy_cost.setLuzboaPrices (data_ingest.luzBoa_data)

    records = energy_cost.get_profile_by_period(data_ingest.profile_data, start_date, end_date)
    
    app.logger.info("End Service: getProfilePeriod")
    return jsonify({
        'start_date' : start_date.strftime("%Y-%d-%m %H:%M"),
        'end_date' : end_date.strftime("%Y-%d-%m %H:%M"),
        'records' : records
    }), 200


################### MAIN ############################
currentDir = os.path.dirname(os.path.abspath(__file__))
BASE_PATH = os.path.join(currentDir, '..' )
OMIE_PATH = os.path.join(BASE_PATH, 'OMIE_Data/')
CONSUMOS_PATH = os.path.join(BASE_PATH, 'Consumos/')
EREDES_PATH = os.path.join(BASE_PATH, 'ERedes_profiles/')

CONSUMO_PROFILE_FILE    = os.path.join(EREDES_PATH, 'E-REDES_Perfil_Consumo_2023_mod.xlsx')
LOSS_PROFILE_FILE       = os.path.join(EREDES_PATH, 'E-REDES_Perfil_Perdas_2023_mod.xlsx')


data_ingest = DataIngestion(OMIE_PATH, CONSUMO_PROFILE_FILE, LOSS_PROFILE_FILE )
initialize_app()

if __name__ == '__main__':
    # start FLASK server
    app.run(host="0.0.0.0", port=5000, debug=True, use_reloader=False)
    
