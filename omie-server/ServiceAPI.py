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
from flask_caching import Cache
from logger_config import setup_logger, setBasePath

from Calculations import EnergyCosts, Energy_Time_Cycle, ENERGY_SUPPLIERS_PARAMETERS, ENERGY_PROFILES
from Ingestion import DataIngestion
from EoT import EoTData

###################  FOLDERS  ############################
currentDir = os.path.dirname(os.path.abspath(__file__))
omie_data_path = os.path.join(currentDir, 'OMIE_Data')
if os.path.exists(omie_data_path):
    BASE_PATH = currentDir
else:
    BASE_PATH = os.path.dirname(currentDir.rstrip('/'))

OMIE_PATH                       = os.path.join(BASE_PATH, 'OMIE_Data/')
CONSUMOS_PATH                   = os.path.join(BASE_PATH, 'Consumos/')
EOT_PATH                        = os.path.join(BASE_PATH, 'EOT/')
EREDES_LOSS_PATH                = os.path.join(BASE_PATH, 'ERedes_loss_profiles/')
EREDES_CONSUMPTION_PATH         = os.path.join(BASE_PATH, 'ERedes_consumption_profiles/')
MASTER_PRICES_FILE              = os.path.join(BASE_PATH, 'master_data.feather')
EOT_CONFIG_FILE                 = os.path.join(EOT_PATH, 'EOT_config.dat')
EOT_DATA_FILE                   = os.path.join(EOT_PATH, 'EOT_data.feather')
CONSUMPTION_PROFILE_TEMPLATE    = 'ERSE_perfis_consumo_'
LOSS_PROFILE_TEMPLATE           = 'E-REDES_Perfil_Perdas_'


###################  FLASK INIT  ############################
# Configure caching
cache_config = {
    "CACHE_TYPE": "SimpleCache",  # In-memory cache for development
    "CACHE_DEFAULT_TIMEOUT": 300  # Cache timeout in seconds (5 minutes)
}
app = Flask(__name__)
CORS(app)
app.config.from_mapping(cache_config)
cache = Cache(app)

#####  HELPER FUNCTIONS  #####
def hour_based_cache_key(*args, **kwargs):
    # Get the current hour
    current_hour = datetime.datetime.now().strftime('%Y-%m-%d_%H')

    # Convert the request.args dictionary into a sorted string
    request_args_str = '-'.join(f"{key}={value}" for key, value in sorted(request.args.items()))

    # Combine the current hour with the request arguments to form the cache key
    cache_key = f"{current_hour}-{request_args_str}"
    return cache_key

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

def generate_dates_period(fromDate, toDate):
    # Define a timedelta of one day
    one_day = timedelta(days=1)

    # Initialize a list to hold the filenames
    filenames = []

    # Loop over the range of dates from max_date to today
    date = fromDate.date()
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
                app.logger.info ('Downloading OMIE data for date: ' + str(req_date) )
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

def log_ip_address():
    client_ip = request.remote_addr
    current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    api_call = f'{request.method} {request.path}?{request.query_string.decode("utf-8")}'
    
    app.logger.info(f'IP: {client_ip} | Endpoint: {request.endpoint} | Path: {request.path} | Method: {request.method}')
    
    global ip_data
    existing_entry = ip_data[ip_data['IP Address'] == client_ip]

    if existing_entry.empty:
        # IP address not found, create a new entry
        new_entry = pd.DataFrame({
            'IP Address': client_ip,
            'First Request Date': current_time,
            'Last Access Date': current_time,
            'API Call': [api_call],
        })
        ip_data = pd.concat([ip_data, new_entry], ignore_index=True)
    else:        
        # Update the existing entry
        index = existing_entry.index[0]
        ip_data.at[index, 'Last Access Date'] = current_time
        ip_data.at[index, 'API Call'] = [api_call]

    filename = os.path.join(BASE_PATH, 'ip_data.csv')
    ip_data.to_csv(filename, index=False)
    return None

def clean_folder (folder_path):
    # Iterate through all files in the folder
    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)
        if os.path.isfile(file_path):
            # Delete the file
            os.remove(file_path)

def initialize_app():
    setBasePath(BASE_PATH)
    app.logger = setup_logger('ServiceAPI')

    # Create the IP dataframe
    global ip_data
    ip_data = pd.DataFrame(columns=['IP Address', 'First Request Date', 'Last Access Date', 'API Call'])
    # Link to the app    
    app.before_request(log_ip_address)
    
    # Clean CONSUMOS folder:
    app.logger.info ("Clean files on Consumos folder...")
    clean_folder (CONSUMOS_PATH)

    # Check and Download OMIE data 
    check_and_download_OMIE ()

    # Calc the Master Prices Table
    energy_cost = EnergyCosts('Tri-Horario-Semanal', 'coopernico-base', 8, 'BTN-C')
    data_ingest.master_prices_table = energy_cost.calc_master_table (data_ingest.profile_data, data_ingest.profile_loss_data, data_ingest.omie_data, MASTER_PRICES_FILE)

    # Start polling of EoT Consumption data
    eot_man.start(60, data_ingest.master_prices_table)


    #### Init testing

    # Force reload all data from date
    # eot_man.GetEoTData ('2023-12-08 00:00')

    #start_date  = datetime.datetime( 2023, 1, 1, 0,0,0)
    #end_date    = datetime.datetime( 2023, 2, 1, 23, 59, 59)
    
    ## download files from 01-01-2023 to 01-02-2023
    #dates = generate_dates_period (start_date, end_date)
    #for req_date in dates:
    #    app.logger.info ('Downloading OMIE data for date: ' + str(req_date) )
    #    data_ingest.download_OMIE_data(req_date)    




################### MAIN ############################
ip_data = None
data_ingest = DataIngestion(BASE_PATH, OMIE_PATH, EREDES_CONSUMPTION_PATH, EREDES_LOSS_PATH, CONSUMPTION_PROFILE_TEMPLATE, LOSS_PROFILE_TEMPLATE )
eot_man = EoTData (BASE_PATH, EOT_CONFIG_FILE, EOT_DATA_FILE)
initialize_app()


############## API REST ################################# 
@app.route('/refreshOMIE', methods=['GET'])
def refresh_omie_data():
    # test with:
    # curl -X GET "http://127.0.0.1:5000/refreshOMIE?try_download=True"
    app.logger.info ("Service: refresh_omie_data - Request parameters: " + str(request.args.to_dict()))

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
    # curl -X GET "http://127.0.0.1:5000/DownloadOMIE?date=2021-02-23"
    # curl -X GET "http://127.0.0.1:5000/DownloadOMIE?filename=data.csv"
    # curl -X GET "http://127.0.0.1:5000/DownloadOMIE?filename=data.csv&overwrite_name=new_name.csv"
    app.logger.info("Service: download_omie_data  - Request parameters: " + str(request.args.to_dict()))

    str_date = request.args.get('date')
    filename = request.args.get('filename')
    overwrite_name = request.args.get('overwrite_name')

    if not str_date and not filename:
        app.logger.error('download_omie_data: Invalid request data')
        return jsonify({'error': 'Invalid request data'}), 400

    if str_date:
        date = datetime.datetime.strptime(str_date, '%Y-%m-%d')
        status = data_ingest.download_OMIE_data(date=date, mode=0)
    elif filename:
        status = data_ingest.download_OMIE_data(filename=filename, overwrite_name=overwrite_name, mode=1)

    if status['saved'] == True:
        status['message'] = 'OMIE data updated successfully'
    else:
        status['message'] = 'OMIE data not updated'    

    app.logger.info("Service Done - downloadOMIE")
    return jsonify(status), 200

@app.route('/uploadEnergyFile', methods=['POST'])
def upload_energy_file():
    # Test with:
    # curl -X POST -F "file=@./ERedes_PT166184330_Fev23.xlsx"  "http://127.0.0.1:5000/uploadEnergyFile?supplier=coopernico-base&tariff=Tri-Horario-Semanal&cycle_day=1&format=json&provider=E-Redes"
    app.logger.info ("Service: upload_energy_file - Request parameters: " + str(request.args.to_dict()) )

    str_supplier = request.args.get('supplier')
    if str_supplier not in ENERGY_SUPPLIERS_PARAMETERS:
        app.logger.error('upload_energy_file: Invalid Energy Supplier')
        return jsonify({'error': 'Invalid Energy Supplier'}), 400

    str_tarifario = request.args.get('tariff')
    if (str_tarifario not in Energy_Time_Cycle):
        app.logger.error('upload_energy_file: Invalid tariff')
        return jsonify({'error': 'Invalid tariff'}), 400
    
    str_exportformat = request.args.get('format')
    if not str_exportformat or str_exportformat not in {'xls', 'csv', 'json'}:
        app.logger.error('upload_energy_file: Invalid export format')
        return jsonify({'error': 'Invalid export format'}), 400

    str_provider = request.args.get('provider')
    if not str_provider or str_provider not in {'E-Redes', 'EoT' }:
        app.logger.error('upload_energy_file: Invalid provider')
        return jsonify({'error': 'Invalid provider'}), 400

    arg_profile = request.args.get('profile')
    if (not arg_profile) or (arg_profile not in ENERGY_PROFILES) :
        app.logger.error ("upload_energy_file: Invalid profile type: " + str(arg_profile))
        return jsonify({'error': 'Invalid profile type'}), 400

    arg_sampling = request.args.get('resampling')
    if arg_sampling is None or arg_sampling == '':
        arg_sampling = ''
    elif arg_sampling is not None and arg_sampling not in ['1H', '1D', '1W']:
        app.logger.error("upload_energy_file: Invalid sampling option: " + str(arg_sampling))
        return jsonify({'error': 'Invalid sampling option'}), 400

    arg_cycle_day = request.args.get('cycle_day')
    if (not arg_cycle_day) or (not arg_cycle_day.isdigit()) or (int(arg_cycle_day) < 0) or (int(arg_cycle_day) > 31) :
        app.logger.error ("upload_energy_file: Invalid cycle_day" + str(arg_cycle_day))
        return jsonify({'error': 'Invalid cycle_day'}), 400
    cycle_day = int(arg_cycle_day)

    arg_lagHour = request.args.get('lagHour')
    laghour = 1
    if (arg_lagHour)  :
        laghour = int(arg_lagHour)

    arg_divisionfactor = request.args.get('divisor')
    if (arg_divisionfactor is None) or (arg_divisionfactor == '') :
        divisionFactor = 0
    elif (arg_divisionfactor is not None) and (not arg_divisionfactor.isdigit()) :
        app.logger.error ("upload_energy_file: Invalid division factor" + arg_divisionfactor)
        return jsonify({'error': 'Invalid division factor'}), 400
    else:
        divisionFactor = int(arg_divisionfactor)

    arg_dataType = request.args.get('datatype')
    if (arg_dataType is None) or (arg_dataType == '') :
        datatype = 'cr'
    elif (arg_dataType is not None) and (not arg_dataType in {'cr','cm'} ) :
        app.logger.error ("upload_energy_file: Invalid data type" + str (arg_dataType) )
        return jsonify({'error': 'Invalid data type'}), 400
    else:
        datatype = arg_dataType

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
    energy_cost = EnergyCosts(str_tarifario, str_supplier, cycle_day, arg_profile )
    # Check if OMIE data is updated
    check_and_download_OMIE ()
    data_ingest.master_prices_table = energy_cost.calc_master_table (data_ingest.profile_data, data_ingest.profile_loss_data, data_ingest.omie_data, MASTER_PRICES_FILE, data_ingest.master_prices_table)
    
    contagens = ''
    if (str_provider == 'E-Redes'):
        # Load the file into a DataFrame
        dfConsumo, contagens = data_ingest.load_ERedes_Consumption_data (filepath, arg_sampling, datatype)
        divisionFactor = 4 if divisionFactor == 0 else divisionFactor
    elif (str_provider == 'EoT'):
        # Load the file into a DataFrame
        dfConsumo = data_ingest.load_EOT_Consumption_data (filepath)
        divisionFactor = 1 if divisionFactor == 0 else divisionFactor

    # Add the energy cost column
    if (dfConsumo is not None):
        dfConsumo = energy_cost.add_energy_consumption_cost_column (dfConsumo, 'Energy', divisionFactor)
    else:
        app.logger.error('upload_energy_file: File is not valid: ' + filepath)
        return jsonify({'error': 'File is not valid'}), 400
        
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
        start = dfConsumo.index.min()
        end = dfConsumo.index.max()

        data = {
            'records': records, 
            'consumo': '' if len(contagens) == 0 else energy_cost.calc_contagem (start, end, contagens),
        }
    
    app.logger.info ("Service Done - uploadEnergyFile")
    return data, 200

@app.route('/getCurrentPrice', methods=['GET'])
@cache.memoize(make_name=hour_based_cache_key)
def get_current_price():
    # test with:
    # curl -X GET "http://127.0.0.1:5000/getCurrentPrice?supplier=coopernico-base&tariff=Tri-Horario-Semanal&cycle_day=1"
    app.logger.info ("Service: getCurrentPrice - Request parameters: " + str(request.args.to_dict()))

    str_supplier = request.args.get('supplier')
    if str_supplier not in ENERGY_SUPPLIERS_PARAMETERS:
        app.logger.error ("get_current_price: Invalid Energy Supplier")
        return jsonify({'error': 'Invalid Energy Supplier'}), 400

    str_tarifario = request.args.get('tariff')
    if (str_tarifario not in Energy_Time_Cycle):
        app.logger.error ("get_current_price: Invalid tariff")
        return jsonify({'error': 'Invalid tariff'}), 400
    
    arg_cycle_day = request.args.get('cycle_day')
    if (not arg_cycle_day) or (not arg_cycle_day.isdigit()) or (int(arg_cycle_day) < 0) or (int(arg_cycle_day) > 31) :
        app.logger.error ("get_current_price: Invalid cycle_day" + str(arg_cycle_day))
        return jsonify({'error': 'Invalid cycle_day'}), 400
    cycle_day = int(arg_cycle_day)

    arg_profile = request.args.get('profile')
    if (not arg_profile) or (arg_profile not in ENERGY_PROFILES) :
        app.logger.error ("get_current_price: Invalid profile type: " + str(arg_profile))
        return jsonify({'error': 'Invalid profile type'}), 400
    # arg_profile = arg_profile.replace('-', ' ')

    arg_lagHour = request.args.get('lagHour')
    laghour = 1
    if (arg_lagHour)  :
        laghour = int(arg_lagHour)

    # Initialize the EnergyCosts class
    energy_cost = EnergyCosts(str_tarifario, str_supplier, cycle_day, arg_profile )
    # Check if OMIE data is updated
    check_and_download_OMIE ()
    data_ingest.master_prices_table = energy_cost.calc_master_table (data_ingest.profile_data, data_ingest.profile_loss_data, data_ingest.omie_data, MASTER_PRICES_FILE, data_ingest.master_prices_table)

    date = datetime.datetime.now() 
    if date > data_ingest.omie_data.index.max():
        app.logger.error ("No OMIE data found for today")
        return jsonify({'error': 'No OMIE data found for today', 'Date':date }), 404
    
    if ( 'luzboa' in str_supplier):
        energy_cost.update_luzboa_cache_prices ()
    
    # Get Prices using Master Table
    new_data = energy_cost.get_current_price (data_ingest.master_prices_table, date, laghour)

    ret = {
        'date': date.strftime('%Y-%m-%d %H:%M'), 
        'net price': new_data['Cost'], 
        'OMIE price': new_data['OMIE_Price'], 
        'OMIE Data': '',
        'Tariff': new_data['Tariff'],
        'Supplier': new_data['Supplier'],
        'Cycle Period': new_data['Cycle Period'],
        'TAR': new_data['TAR'],
        'TAR Period': new_data['TAR Period'] 
    }

    app.logger.info ("Service Done - getCurrentPrice")
    return jsonify( ret ), 200

@app.route('/getOMIEPricesForPeriod', methods=['GET'])
@cache.memoize(make_name=hour_based_cache_key)
def getOMIEPricesForPeriod ():
    # test with:
    # curl -X GET "http://127.0.0.1:5000/getOMIEPricesForPeriod?supplier=coopernico-base&tariff=Simples&cycle_day=1&supplier=coopernico-base&start_date=2023-02-23-00:00&end_date=2023-02-24-00:00"
    app.logger.info ("Service: getOMIEPricesForPeriod - Request parameters: " + str(request.args.to_dict()))

    str_supplier = request.args.get('supplier')
    if str_supplier not in ENERGY_SUPPLIERS_PARAMETERS:
        app.logger.error ("getOMIEPricesForPeriod: Invalid Energy Supplier")
        return jsonify({'error': 'Invalid Energy Supplier'}), 400

    str_tarifario = request.args.get('tariff')
    if (str_tarifario not in Energy_Time_Cycle):
        app.logger.error ("getOMIEPricesForPeriod: Invalid tariff")
        return jsonify({'error': 'Invalid tariff'}), 400
    
    arg_cycle_day = request.args.get('cycle_day')
    if (not arg_cycle_day) or (not arg_cycle_day.isdigit()) or (int(arg_cycle_day) < 0) or (int(arg_cycle_day) > 31) :
        app.logger.error ('getOMIEPricesForPeriod: Invalid cycle_day' + str(arg_cycle_day) )
        return jsonify({'error': 'Invalid cycle_day'}), 400
    cycle_day = int(arg_cycle_day)    

    arg_profile = request.args.get('profile')
    if (not arg_profile) or (arg_profile not in ENERGY_PROFILES) :
        app.logger.error ("getOMIEPricesForPeriod: Invalid profile type: " + str(arg_profile))
        return jsonify({'error': 'Invalid profile type'}), 400
    # arg_profile = arg_profile.replace('-', ' ')

    # Initialize the EnergyCosts class
    energy_cost = EnergyCosts(str_tarifario, str_supplier, cycle_day, arg_profile )
    # Check if OMIE data is updated
    check_and_download_OMIE ()
    data_ingest.master_prices_table = energy_cost.calc_master_table (data_ingest.profile_data, data_ingest.profile_loss_data, data_ingest.omie_data, MASTER_PRICES_FILE, data_ingest.master_prices_table)

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

    # Get prices using Master Table
    prices_for_period, prices_df, records_df = energy_cost.get_energy_cost_between_dates(data_ingest.master_prices_table, start_date,  end_date, laghour)

    if prices_for_period.empty:
        app.logger.error ("No OMIE data found for the given period")
        return jsonify({'error': 'No OMIE data found for the given period'}), 404

    prices_df.reset_index(inplace=True)
    prices_json = prices_df.to_json(orient='records', date_format='iso')
    prices_dict = json.loads(prices_json)

    records_df.reset_index(inplace=True)
    records_json = records_df.to_json(orient='records', date_format='iso')
    records = json.loads(records_json)
    
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
    # curl -X GET "http://127.0.0.1:5000/getEstimationProfile?tariff=Tri-Horario-Semanal&cycle_day=1&supplier=coopernico-base&start_date=2023-02-23-00:00&end_date=2023-02-24-00:00&total_energy=655.32"
    app.logger.info ("Service: getEstimationProfile - Request parameters: " + str(request.args.to_dict()))

    str_supplier = request.args.get('supplier')
    if str_supplier not in ENERGY_SUPPLIERS_PARAMETERS:
        app.logger.error ("estimate_profile_cost: Invalid Energy Supplier")
        return jsonify({'error': 'Invalid Energy Supplier'}), 400

    str_tarifario = request.args.get('tariff')
    if (str_tarifario not in Energy_Time_Cycle):
        app.logger.error ("estimate_profile_cost: Invalid tariff")
        return jsonify({'error': 'Invalid tariff'}), 400
    
    arg_cycle_day = request.args.get('cycle_day')
    if (not arg_cycle_day) or (not arg_cycle_day.isdigit()) or (int(arg_cycle_day) < 0) or (int(arg_cycle_day) > 31) :
        app.logger.error ("estimate_profile_cost: Invalid cycle_day" + str(arg_cycle_day) )
        return jsonify({'error': 'Invalid cycle_day'}), 400
    cycle_day = int(arg_cycle_day)

    arg_profile = request.args.get('profile')
    if (not arg_profile) or (arg_profile not in ENERGY_PROFILES) :
        app.logger.error ("getEstimationProfile: Invalid profile type: " + str(arg_profile))
        return jsonify({'error': 'Invalid profile type'}), 400
    # arg_profile = arg_profile.replace('-', ' ')

    # Initialize the EnergyCosts class
    energy_cost = EnergyCosts(str_tarifario, str_supplier, cycle_day, arg_profile )
    # Check if OMIE data is updated
    check_and_download_OMIE ()
    data_ingest.master_prices_table = energy_cost.calc_master_table (data_ingest.profile_data, data_ingest.profile_loss_data, data_ingest.omie_data, MASTER_PRICES_FILE, data_ingest.master_prices_table)

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
    # curl -X GET "http://127.0.0.1:5000/getEstimationProfile?tariff=Tri-Horario-Semanal&start_date=2023-02-23-00:00&end_date=2023-02-24-00:00&total_vazio=155.32&total_cheio=155.32&total_ponta=155.32&cycle_day=1&supplier=coopernico-base"
    app.logger.info ("Service: getEstimationProfileManual - Request parameters: " + str(request.args.to_dict()))

    str_supplier = request.args.get('supplier')
    if str_supplier not in ENERGY_SUPPLIERS_PARAMETERS:
        app.logger.error("estimate_profile_cost_manual: Invalid Energy Supplier")
        return jsonify({'error': 'Invalid Energy Supplier'}), 400

    str_tarifario = request.args.get('tariff')
    if (str_tarifario not in Energy_Time_Cycle):
        app.logger.error("estimate_profile_cost_manual: Invalid tariff")
        return jsonify({'error': 'Invalid tariff'}), 400
        
    arg_cycle_day = request.args.get('cycle_day')
    if (not arg_cycle_day) or (not arg_cycle_day.isdigit()) or (int(arg_cycle_day) < 0) or (int(arg_cycle_day) > 31) :
        app.logger.error ("estimate_profile_cost_manual: Invalid cycle_day" + str(arg_cycle_day))
        return jsonify({'error': 'Invalid cycle_day'}), 400
    cycle_day = int(arg_cycle_day)    

    arg_profile = request.args.get('profile')
    if (not arg_profile) or (arg_profile not in ENERGY_PROFILES) :
        app.logger.error ("getEstimationProfileManual: Invalid profile type: " + str(arg_profile))
        return jsonify({'error': 'Invalid profile type'}), 400
    # arg_profile = arg_profile.replace('-', ' ')

    # Initialize the EnergyCosts class
    energy_cost = EnergyCosts(str_tarifario, str_supplier, cycle_day, arg_profile )
    # Check if OMIE data is updated
    check_and_download_OMIE ()
    data_ingest.master_prices_table = energy_cost.calc_master_table (data_ingest.profile_data, data_ingest.profile_loss_data, data_ingest.omie_data, MASTER_PRICES_FILE, data_ingest.master_prices_table)

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
    
    if start_date < data_ingest.omie_data.index.min() :
        app.logger.error("Invalid date range on OMIE data")
        return jsonify({'error': 'Invalid date range on OMIE data'}), 400

    if end_date > data_ingest.omie_data.index.max():
        end_date = data_ingest.omie_data.index.max()

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
    
    response = ''
    if ("coopernico" in str_supplier):
        response = energy_cost.calc_coopernico_costs (start_date, end_date, total_vazio, total_cheio, total_ponta, laghour, records)
    elif ("luzboa" in str_supplier):
        response = energy_cost.calc_luzboa_costs (start_date, end_date, total_vazio, total_cheio, total_ponta, laghour, records)
    else:
        app.logger.error("Invalid Energy Supplier")
        return jsonify({'error': 'Invalid Energy Supplier'}), 400    

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
    # curl -X GET "http://127.0.0.1:5000/getEOTData?key=XXXXXXXXX"
    app.logger.info ("Start Service: getEOTData - Request parameters: " + str(request.args.to_dict()) )

    str_eotKey = request.args.get('key')
    if (not str_eotKey) :
        app.logger.error("get_eot_data: Invalid EOT Key")
        return jsonify({'error': 'Invalid Key'}), 400

    user_id = str_eotKey
    cycle_day = eot_man.config_dict[user_id]['cycle_day']

    date_power  = eot_man.lastEoTdate_power
    power       = eot_man.lastEoTpower

    current_day_energy, current_day_cost, current_day_max_counter = eot_man.GetCurrentDayTotal (user_id)
    current_month_energy, current_month_cost, _ = eot_man.GetCurrentMonthTotal (user_id, cycle_day)

    app.logger.info("End Service: getEOTData")
    return jsonify({'date': date_power, 
                    'power': power,  
                    'energy_last': current_day_max_counter,
                    'energy_total': current_day_energy,
                    'total_cost': current_day_cost,
                    'month_energy_total': current_month_energy,
                    'month_total_cost': current_month_cost}), 200    



if __name__ == '__main__':
    # start FLASK server
    app.logger.info ("FLASK Server Starting...")
    app.run(host="0.0.0.0", port=15000, debug=True, use_reloader=False)