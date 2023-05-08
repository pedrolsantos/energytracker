// Config properties
var CONFIG = {
                'api_url': '',  
                'tariff': 'Tri-Horario-Semanal',    
                'year': '2023',   
                'curva_perfil_c': "True",
                'supplier': 'coopernico-base',                  
                'chart_update_interval': 15,    // 15 minute default
                'chart_update_period': 12,      // 12 hours default  
                'chart_update_period_net': 12,  // 12 hours default    
                'EoTKey': 'XXXXXXXXXX',
                'cycle_day': 8,
                'profile': 'BTN-C',
                'divisor': 0,
                'allowDebug': false,
            };


// ********************* MAIN CODE *********************
// Class to manage the API calls
class ApiService {
    constructor(baseUrl) {
      this.baseUrl = baseUrl;
    }
    
    async getCurrentPrice(){
      try {
        const supplier = CONFIG.supplier;
        const tariff = CONFIG.tariff;
        const year = CONFIG.year;
        const cycle_day = CONFIG.cycle_day;
        const profile = CONFIG.profile;

        const response = await fetch(`${this.baseUrl}/getCurrentPrice?tariff=${tariff}&year=${year}&supplier=${supplier}&cycle_day=${cycle_day}&profile=${profile}`);
  
        if (!response.ok) {
          throw new Error(`HTTP error: ${response.status}`);
        }
  
        const data = await response.json();
        return data;
      } catch (error) {
        console.error(`Error fetching current price: ${error}`);
        throw error;
      }
    }

    async getOMIEPricesForPeriod(startDate, endDate ) {
        try {
            const supplier = CONFIG.supplier;
            const tariff = CONFIG.tariff;
            const year = CONFIG.year;
            const cycle_day = CONFIG.cycle_day;
            const profile = CONFIG.profile;
    
            const response = await fetch(
                `${this.baseUrl}/getOMIEPricesForPeriod?tariff=${tariff}&supplier=${supplier}&year=${year}&cycle_day=${cycle_day}&profile=${profile}&start_date=${startDate}&end_date=${endDate}`
            );

            if (!response.ok) {
                throw new Error(`HTTP error: ${response.status}`);
            }

            const data = await response.json();
            return data;
        } catch (error) {
            console.error(`Error fetching OMIE prices for period: ${error}`);
            throw error;
        }
    }      

    async uploadEnergyFile(format, provider, file) {
        const supplier = CONFIG.supplier;
        const tariff = CONFIG.tariff;
        const year = CONFIG.year;
        const cycle_day = CONFIG.cycle_day;
        const profile = CONFIG.profile;
        const resample = sample; 
        const divisor = (CONFIG.divisor != 0) ? CONFIG.divisor: '';
        const lagHour = 0;

        const url = `${this.baseUrl}/uploadEnergyFile?supplier=${supplier}&tariff=${tariff}&year=${year}&cycle_day=${cycle_day}&profile=${profile}&format=${format}&provider=${provider}&resampling=${resample}&divisor=${divisor}&lagHour=${lagHour}`;
    
        const formData = new FormData();
        formData.append('file', file);
    
        const options = {
          method: 'POST',
          body: formData,
        };
    
        try {
          const response = await fetch(url, options);
    
          if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
          }
    
          const jsonResponse = await response.json();
          //logger('async uploadEnergyFile: File uploaded successfully:', jsonResponse);
          return jsonResponse;
        } catch (error) {
          console.error('Error uploading file:', error);
          throw error;
        }
    }

    async getEstimationProfile(start_date, end_date, total_energy, records='none') {
        try {
            const supplier = CONFIG.supplier;
            const tariff = CONFIG.tariff;
            const year = CONFIG.year;
            const cycle_day = CONFIG.cycle_day;
            const profile = CONFIG.profile;

            const response = await fetch(`${this.baseUrl}/getEstimationProfile?supplier=${supplier}&tariff=${tariff}&year=${year}&cycle_day=${cycle_day}&profile=${profile}&start_date=${start_date}&end_date=${end_date}&total_energy=${total_energy}&records=${records}`);
            if (!response.ok) {
                throw new Error(`HTTP error: ${response.status}`);
            }
            const data = await response.json();
            return data;
        } catch (error) {
            console.error(`Error fetching estimation profile: ${error}`);
            throw error;
        }
    }    

    async getEstimationProfileManual(start_date, end_date, total_vazio, total_cheio, total_ponta, lagHour=0, records='none' ) {
        try {
            const supplier = CONFIG.supplier;
            const tariff = CONFIG.tariff;
            const year = CONFIG.year;
            const cycle_day = CONFIG.cycle_day;
            const profile = CONFIG.profile;

            const response = await fetch(`${this.baseUrl}/getEstimationProfileManual?supplier=${supplier}&tariff=${tariff}&year=${year}&cycle_day=${cycle_day}&profile=${profile}&start_date=${start_date}&end_date=${end_date}&total_vazio=${total_vazio}&total_cheio=${total_cheio}&total_ponta=${total_ponta}&lagHour=${lagHour}&records=${records}`);
            if (!response.ok) {
                throw new Error(`HTTP error: ${response.status}`);
            }
            const data = await response.json();
            return data;
        } catch (error) {
            console.error(`Error fetching estimation profile: ${error}`);
            throw error;
        }
    }        

    async getEOTData() {
        try {
            const supplier = CONFIG.supplier;
            const tariff = CONFIG.tariff;
            const year = CONFIG.year;
            const cycle_day = CONFIG.cycle_day;
            const key = CONFIG.EoTKey;
            const profile = CONFIG.profile;

            const response = await fetch(`${this.baseUrl}/getEOTData?supplier=${supplier}&tariff=${tariff}&year=${year}&cycle_day=${cycle_day}&profile=${profile}&key=${key}`);
            if (!response.ok) {
                throw new Error(`HTTP error: ${response.status}`);
            }
            const data = await response.json();
            return data;
        } catch (error) {
            console.error(`Error fetching EOT data: ${error}`);
            throw error;
        }
    }

}

// Class to manage the APP Functions
class PriceTracker {
    constructor(apiService, chartUpdatePeriod) {
        this.apiService = apiService;
        this.chartUpdatePeriod = chartUpdatePeriod;
        this.lastUpdateChart = new Date();

        this.lastRealDataFetch = null;
        this.lastEstimateDataFetch = null;

        this.lastRealDataDisplay = null;
        this.lastEstimateDataDisplay = null;
        this.analysisChart = null

        this.isRealConsumption = true;

        this.zoomOptions = {
            pan: {
                enabled: true,
                mode: 'x',
            },
            zoom: {
                wheel: {
                    enabled: true,
                },
                pinch: {
                    enabled: true
                },
                drag: {
                    enabled: false
                },              
                mode: 'x'
            }
        };
        
        this.chartConfig = {
            type: 'bar',
            data: {
                labels:[],
                datasets: [{
                    label: 'Energia',
                    data: [],
                    borderColor: 'rgba(75, 192, 192, 1)',
                    backgroundColor: 'rgba(75, 192, 192, 0.2)',
                    borderWidth: 1,
                    tension: 0.1,
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                scales: {
                    x: {
                        title: {
                            display: true,
                            text: 'Hora'
                        }
                    },
                    y: {
                        title: {
                            display: true,
                            text: 'Preço'
                        }
                    }
                },
                plugins: {
                    tooltip: {
                        callbacks: {
                            title: function (context) {
                                let title = 'Hora: ' + context[0].label + ':00';
                                return title;                            
                            },
                            label: function (context) {
                                let label = context.dataset.label || '';
                                if (label) {
                                    label = 'Preço: ';
                                }
                                label += Number(context.parsed.y).toFixed(2) + ' €/MWh';
                                return label;                            
                            }
                        }
                    }              
                }
            }             
        };

        this.chartConfigNet = {
            type: 'bar',
            data: {
                labels:[],
                datasets: [{
                    label: 'Energia',
                    data: [],
                    borderColor: 'rgba(75, 192, 192, 1)',
                    backgroundColor: 'rgba(75, 192, 192, 0.2)',
                    borderWidth: 1,
                    tension: 0.1,
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                scales: {
                    x: {
                        title: {
                            display: true,
                            text: 'Hora'
                        }
                    },
                    y: {
                        title: {
                            display: true,
                            text: 'Preço'
                        }
                    }
                },
                plugins: {
                    tooltip: {
                        callbacks: {
                            title: function (context) {
                                let title = 'Hora: ' + context[0].label + ':00';
                                return title;                            
                            },
                            label: function (context) {
                                let label = context.dataset.label || '';
                                if (label) {
                                    label = 'Preço: ';
                                }
                                label += Number(context.parsed.y).toFixed(4) + ' €/kWh';
                                return label;                            
                            }
                        }
                    }              
                }
    
            },          
                    
        };
    }

    initCharts() {
        const chartCanvas = document.getElementById('price-chart');
        const priceChart = new Chart(chartCanvas, this.chartConfig);
    
        const chartCanvasNet = document.getElementById('price-chart-net');
        const priceChartNet = new Chart(chartCanvasNet,  this.chartConfigNet);

        const chartCanvasAnalysis = document.getElementById('price-chart-analysis');
        const priceChartAnalysis = new Chart(chartCanvasAnalysis,  {type:'line'});
       
        return [priceChart, priceChartNet, priceChartAnalysis];
    }

    // Helper function to capitalize the first letter of each word in a string
    formatTitleCase(str) {
        return str
        .toLowerCase()
        .split('-')
        .map(word => word.charAt(0).toUpperCase() + word.slice(1))
        .join('-');
    }

    setAnalysisPageforSupplier(supplier, tariff) {
        const now = new Date();
        const start_date  = new Date(now.getFullYear(), now.getMonth(), now.getDate() - 2, 0, 0);
        const end_date = new Date(now.getFullYear(), now.getMonth(), now.getDate() - 1, 23, 59);

        document.getElementById('analysis-comercializador').textContent = this.formatTitleCase (supplier);
        document.getElementById('analysis-opcao-horaria').textContent = this.formatTitleCase (tariff);

        const isLuzboa = (supplier === 'luzboa-spot') ? true : false;

        if (isLuzboa) {
            document.getElementById('title_profile').textContent = "Cálculo do Consumo :"
            document.getElementById('analysis-periodo-start').value = this.formatDate(start_date, true)
            document.getElementById('analysis-periodo-end').value = this.formatDate(end_date, true);            
        }
        else{
            document.getElementById('title_profile').textContent = "Curva Perfil " + CONFIG.profile
        }

        // Constrol elements according to supplier
        document.getElementById('blockFileProvider').style.display = (isLuzboa)? 'none' : ''
        document.getElementById('blockConsumoEstimado').style.display = (isLuzboa)? 'none' : ''
        document.getElementById('blockChart').style.display = (isLuzboa)? 'none' : ''
        document.getElementById('analysis-periodo-start').disabled = (isLuzboa)? false : true;
        document.getElementById('analysis-periodo-end').disabled = (isLuzboa)? false : true;

    

        // Clear analysis data
        document.getElementById('analysis-energia-profile-vazio-total').textContent = '0.00 kWh (0.00 €)';
        document.getElementById('analysis-energia-profile-cheio-total').textContent = '0.00 kWh (0.00 €)';
        document.getElementById('analysis-energia-profile-ponta-total').textContent = '0.00 kWh (0.00 €)';
        document.getElementById('analysis-energia-profile-total').textContent       = '0.00 kWh';
        document.getElementById('analysis-custo-profile-total').textContent         = '0.00 €';
        document.getElementById('analysis-custo-profile-preco-medio').textContent   = '0.000000 €/kWh';

        document.getElementById('analysis-custo-profile-preco-medio-vazio').textContent = '0.000000 €/kWh';
        document.getElementById('analysis-custo-profile-preco-medio-cheio').textContent = '0.000000 €/kWh';
        document.getElementById('analysis-custo-profile-preco-medio-ponta').textContent = '0.000000 €/kWh';

    }

    fetchServerData( priceChart, priceChartNet, force=false) {
        logger('Fetching Price...' + this.lastUpdateChart);
        var current_price = 0

        this.apiService.getCurrentPrice().then(data => {            
                logger('Current Price:', data);

                const currentDate = new Date();
                current_price = data['net price'];
                const dst = data['TAR Period']['dst'] === 0 ? ' / Inverno' : ' / Verão';
                const dia = data['TAR Period']['dias'] === 0 ? ('Dia de Semana' + dst) : data['TAR Period']['dias'] === 1 ? ('Sábado' + dst) : ('Domingo' + dst);
                const periodo = data['TAR Period']['periodo'] + ': ' + data['TAR Period']['start'] + ' - ' + data['TAR Period']['end'];
                const cycle_period = data['Cycle Period']['start'].substring(0,10) ; //  + ' : ' + data['Cycle Period']['end'].substring(0,10) ;
                const profile = CONFIG.profile;

                document.getElementById('current-price-omie').textContent = `${data['OMIE price'].toFixed(2)} €/MWh`;
                document.getElementById('current-price-net').textContent = `${data['net price'].toFixed(4)} €/kWh`;
                document.getElementById('current-profile').textContent = profile;
                
                document.getElementById('current-cycle_period').textContent = `${cycle_period}`;
                
                document.getElementById('current-tarifario').textContent = `${data['Tariff']} `;
                document.getElementById('current-supplier').textContent = `${this.formatTitleCase( data['Supplier'] )} `;

                document.getElementById('current-dia').textContent = `${dia} `;
                document.getElementById('current-periodo').textContent = `${periodo} `;
                document.getElementById('current-time').textContent = currentDate.toLocaleTimeString('en-US', { hour12: false });
            })
            .catch(error => {
                console.error('Error:', error);
            }
        );

        if (CONFIG.EoTKey !== '' && !/^X*$/.test(CONFIG.EoTKey)) {
            this.apiService.getEOTData().then( data => {
                logger('EoT Data:', data);

                const power    = data['power'];
                const energy   = data['energy_total'];
                const cost     = data['total_cost'];

                document.getElementById('current-power').textContent = `${power} Watts `; 
                document.getElementById('current-consumo-hora').textContent = `${energy.toFixed(2)} kWh (${cost.toFixed(4)} €)`;

            })
            .catch(error => {
                console.error('Error:', error);
            } );
        }
        else{
            document.getElementById('current-power').textContent = `N/A`;
            document.getElementById('current-consumo-hora').textContent = `N/A`;    
        }
      
        // check if the chart needs to be updated (every hour)
        const now = new Date();
        if ( (now.getHours() !== this.lastUpdateChart.getHours()) || (force==true)) {
            this.lastUpdateChart = this.updateChartPeriod(priceChart, priceChartNet); 
        }
    }   

    formatDate(date, toshow=false) {
        const year = date.getFullYear();
        const month = String(date.getMonth() + 1).padStart(2, "0");
        const day = String(date.getDate()).padStart(2, "0");
        const hour = String(date.getHours()).padStart(2, "0");
        const minutes = String(date.getMinutes()).padStart(2, '0');
        if (toshow) {
            return `${year}-${month}-${day} ${hour}:${minutes}`;
        }
        return `${year}-${month}-${day}-${hour}:${minutes}`;
    }

    getColor(price, minPrice, oneThird, index) {
        let color = 'rgba(75, 192, 192, 0.2)';
        let alpha = 0.25;
        if (index) { 
            alpha = 1; 
        }
        if (price < minPrice + oneThird) {
            color = `rgba(0, 100, 0, ${alpha})`; // Green
        } else if (price < minPrice + 2 * oneThird) {
            color = `rgba(255, 165, 0, ${alpha})`; // Orange
        } else {
            color = `rgba(255, 0, 0, ${alpha})`; // Red
        }
        return color
    }

    findIndexByHour(data, targetHour) {
        for (let i = 0; i < data.length; i++) {
            const date = new Date(data[i].Date.slice(0, 16));
            const hour = date.getHours();
            if (hour === targetHour) {
                return i;
            }
        }
        return -1; // Return -1 if the targetHour is not found
    }

    updateChartPeriod(chart, chartnet) {
        const DateTime = luxon.DateTime;
        const hours = this.chartUpdatePeriod
        const now = new Date();
        const currentHour = now.getHours();
        now.setMinutes(0, 0, 0); // Set minutes to 0

        const startDate = new Date(now.getTime() - hours * 60 * 60 * 1000); // 6 hours ago
        const endDate = new Date(now.getTime() + hours * 60 * 60 * 1000);   // 6 hours from now
        logger('Fetching period from:[' + startDate + '] to:[' + endDate + '] date=[' + now + ']');

        this.apiService.getOMIEPricesForPeriod( this.formatDate(startDate), this.formatDate(endDate))
            .then(data => {                
                logger('OMIE Prices:', data);
                var prices = data['prices']
                var profile = data['profile']

                // Get the current hour index on the prices array
                const indexHour = this.findIndexByHour (prices, currentHour);

                // Get the min and max prices
                const minPrice = Math.min(...prices.map(entry => entry.Price_PT));
                const maxPrice = Math.max(...prices.map(entry => entry.Price_PT));
                const minCost = Math.min(...prices.map(entry => entry.Cost));
                const maxCost = Math.max(...prices.map(entry => entry.Cost));
            
                // ****************************************************  OMIE CHART ****************************************************************
                // Set the chart data
                chart.data.labels = prices.map(entry =>DateTime.fromISO(entry.Date).toUTC().toFormat('HH') );
                chart.data.datasets[0].label = 'Energia OMIE';
                chart.data.datasets[0].data = prices.map(entry => entry.Price_PT);
                
                // Set the chart colors
                let oneThird = (maxPrice - minPrice) / 3;                                
                chart.data.datasets[0].backgroundColor =  prices.map((entry, index) => this.getColor(entry.Price_PT, minPrice, oneThird, index==indexHour));
                chart.data.datasets[0].borderColor = prices.map((entry, index) => (index==indexHour) ? 'darkblue' : 'lightgray');
                chart.update();
            
                // **************************************************** COOPERNICO CHART ***********************************************************
                this.loadEnergyChart (chartnet, prices, profile, oneThird, minCost, maxCost, indexHour);
                
            })
            .catch(error => {
                console.error('Error:', error);
            });
    
        return now; // Return the time of the last update    
    }

    getMinMaxDate(array) {
        if (array.length === 0) {
          return { minDate: null, maxDate: null };
        }
      
        let minDate = new Date(array[0].Date);
        let maxDate = new Date(array[0].Date);
      
        for (const item of array) {
          const currentDate = new Date(item.Date);
      
          if (currentDate < minDate) {
            minDate = currentDate;
          }
      
          if (currentDate > maxDate) {
            maxDate = currentDate;
          }
        }
        return { minDate, maxDate };
    }

    processEnergyData(data) {
        let results = {
            vazioCost: 0,
            cheioCost: 0,
            pontaCost: 0,
            totalCost: 0,
            vazioEnergy: 0,
            cheioEnergy: 0,
            pontaEnergy: 0,
            vazioAvgPrice: 0,
            cheioAvgPrice: 0,
            pontaAvgPrice: 0,
            totalEnergy:0,
            globalAvgPrice:0
        };
      
        data.forEach(row => {
            if (row.Vazio !== null) {
                results.vazioCost += row.Cost;
                results.vazioEnergy += row.Vazio;
            }
            if (row.Cheio !== null) {
                results.cheioCost += row.Cost;
                results.cheioEnergy += row.Cheio;
            }
            if (row.Ponta !== null) {
                results.pontaCost += row.Cost;
                results.pontaEnergy += row.Ponta;
            }
            results.totalCost += row.Cost;
        });
      
        results.vazioAvgPrice = results.vazioEnergy ? results.vazioCost / results.vazioEnergy : 0;
        results.cheioAvgPrice = results.cheioEnergy ? results.cheioCost / results.cheioEnergy : 0;
        results.pontaAvgPrice = results.pontaEnergy ? results.pontaCost / results.pontaEnergy : 0;
        results.totalEnergy = results.vazioEnergy + results.cheioEnergy + results.pontaEnergy;
        results.globalAvgPrice = results.totalCost / results.totalEnergy;

        return results;
    }

    getTotalEnergyCost(data){
        const totalEnergyAndCost = data.reduce((accumulator, currentValue) => {
            return {
              sumEnergy: accumulator.sumEnergy + currentValue.Energy,
              sumCost  : accumulator.sumCost + currentValue.Cost,
              sumVazio : accumulator.sumVazio + currentValue.Vazio,
              sumCheio : accumulator.sumCheio + currentValue.Cheio,
              sumPonta : accumulator.sumPonta + currentValue.Ponta
            };
        }, { sumEnergy: 0, sumCost: 0, sumVazio: 0, sumCheio: 0, sumPonta: 0 });
        return totalEnergyAndCost
    }

    updateChartAnalysis(chart, data) {
        document.getElementById('title_consumo_real').textContent = "Consumo Real :"
        // Consumo Real :
        const totalEnergyAndCost = this.getTotalEnergyCost(data); // TO DELETE ******
        const dataResults = this.processEnergyData(data);

        const minmaxdate = this.getMinMaxDate (data)
        const global_minmaxdate = this.getMinMaxDate (this.lastRealDataFetch)

        const total_energy =  totalEnergyAndCost.sumEnergy.toFixed(2)
        const average_price_total = totalEnergyAndCost.sumCost/totalEnergyAndCost.sumEnergy;
        const average_price_vazio = totalEnergyAndCost.sumCost/totalEnergyAndCost.sumVazio;
        const average_price_cheio = totalEnergyAndCost.sumCost/totalEnergyAndCost.sumCheio;
        const average_price_ponta = totalEnergyAndCost.sumCost/totalEnergyAndCost.sumPonta;

        document.getElementById('analysis-periodo-start').value = this.formatDate(global_minmaxdate.minDate, true)
        document.getElementById('analysis-periodo-end').value = this.formatDate(global_minmaxdate.maxDate, true);

        document.getElementById('analysis-custo-total').textContent = `${dataResults.totalCost.toFixed(2)}` + ' €';
        document.getElementById('analysis-custo-preco-medio').textContent = `${ dataResults.globalAvgPrice.toFixed(6)} ` + ' €/kWh';
        
        document.getElementById('analysis-energia-vazio-total').textContent = `${dataResults.vazioEnergy.toFixed(2)} ` + ' kWh' + ' (' + `${dataResults.vazioCost.toFixed(2)} ` + ' €)';
        document.getElementById('analysis-energia-cheio-total').textContent = `${dataResults.cheioEnergy.toFixed(2)} ` + ' kWh' + ' (' + `${dataResults.cheioCost.toFixed(2)} ` + ' €)';
        document.getElementById('analysis-energia-ponta-total').textContent = `${dataResults.pontaEnergy.toFixed(2)} ` + ' kWh' + ' (' + `${dataResults.pontaCost.toFixed(2)} ` + ' €)';
        document.getElementById('analysis-energia-total').textContent = `${dataResults.totalEnergy.toFixed(2)} ` + ' kWh';
        
        document.getElementById('analysis-custo-preco-medio-vazio').textContent = `${dataResults.vazioAvgPrice.toFixed(6)} ` + ' €/kWh';
        document.getElementById('analysis-custo-preco-medio-cheio').textContent = `${dataResults.cheioAvgPrice.toFixed(6)} ` + ' €/kWh';
        document.getElementById('analysis-custo-preco-medio-ponta').textContent = `${dataResults.pontaAvgPrice.toFixed(6)} ` + ' €/kWh';        

        // Call Profile Data based on Switch option
        document.getElementById('title_profile').textContent = "Curva Perfil " + CONFIG.profile + " ..... Em Processamento ...."

        var inputVazio = ( document.getElementById('analysis-contador-vazio').value );
        var inputCheio = ( document.getElementById('analysis-contador-cheio').value );
        var inputPonta = ( document.getElementById('analysis-contador-ponta').value ) ;
        if (inputVazio == 0){
            inputVazio = totalEnergyAndCost.sumVazio.toFixed(2) ;
            document.getElementById('analysis-contador-vazio').value = inputVazio;
        }
        if (inputCheio == 0){
            inputCheio = totalEnergyAndCost.sumCheio.toFixed(2) ;
            document.getElementById('analysis-contador-cheio').value = inputCheio;
        }
        if (inputPonta == 0){
            inputPonta = totalEnergyAndCost.sumPonta.toFixed(2) ;
            document.getElementById('analysis-contador-ponta').value = inputPonta;
        }
        this.fetchProfileDataBasedOnManual(minmaxdate, inputVazio, inputCheio, inputPonta,)
        this.lastRealDataDisplay = data;

        // Prepare and show the chart
        // we can't show the profile results here because it needs to fetch the data in async mode 
        // the profile data is shown inside the response
        if (document.getElementById('switch-chart').checked){
            // Show the Real Consumption
            this.isRealConsumption = true
            this.loadDataOnAnalysisChart(chart, this.lastRealDataDisplay);    
        }

    }

    aggregateByHour(array) {
        const hourlyData = {};
        
        var prevHour = null;
        array.forEach(item => {
            const date = new Date(item.Date);
            const hour = new Date(date.getFullYear(), date.getMonth(), date.getDate(), date.getHours());
            const hourStr = hour.toISOString();
            
            if (!hourlyData[hourStr]) {
                hourlyData[hourStr] = {
                    Date: hourStr,
                    Energy: 0,
                    Vazio: null,
                    Cheio: null,
                    Ponta: null
                };
            }
          
            if (item.Vazio != null) hourlyData[hourStr].Vazio += item.Vazio;
            if (item.Cheio != null) hourlyData[hourStr].Cheio += item.Cheio;
            if (item.Ponta != null) hourlyData[hourStr].Ponta += item.Ponta;   

            hourlyData[hourStr].Energy += item.Energy;
            prevHour = hourStr;


            const variables = [hourlyData[hourStr].Vazio, hourlyData[hourStr].Cheio, hourlyData[hourStr].Ponta];
            const nonNullVariables = variables.filter(value => value !== null);
            
            if (nonNullVariables.length > 0){
                // There is more than one null value
                
                const max = Math.max(...nonNullVariables);
                const min = Math.min(...nonNullVariables);

                if (hourlyData[hourStr].Vazio == max) {
                    if (hourlyData[hourStr].Cheio == min){
                        hourlyData[hourStr].Vazio += min;
                        hourlyData[hourStr].Cheio = null;
                    }
                    else if (hourlyData[hourStr].Ponta == min){
                        hourlyData[hourStr].Vazio += min;
                        hourlyData[hourStr].Ponta = null;
                    }
                }
                else if (hourlyData[hourStr].Cheio == max) {
                    if (hourlyData[hourStr].Vazio == min){
                        hourlyData[hourStr].Cheio += min;
                        hourlyData[hourStr].Vazio = null;
                    }
                    else if (hourlyData[hourStr].Ponta == min){
                        hourlyData[hourStr].Cheio += min;
                        hourlyData[hourStr].Ponta = null;
                    }
                }
                else if (hourlyData[hourStr].Ponta == max) {
                    if (hourlyData[hourStr].Vazio == min){
                        hourlyData[hourStr].Ponta += min;
                        hourlyData[hourStr].Vazio = null;
                    }
                    else if (hourlyData[hourStr].Cheio == min){
                        hourlyData[hourStr].Ponta += min;
                        hourlyData[hourStr].Cheio = null;
                    }
                }
            }


        });
      
        return Object.values(hourlyData);
    }

    loadEnergyChart (chart, prices, profile, oneThird, minCost, maxCost, indexHour) {    
        // Set the chart datasets
        const DateTime = luxon.DateTime;
        oneThird = (maxCost - minCost) / 3;

        var profileHourly = this.aggregateByHour(profile);
        var supplier = (CONFIG.supplier.toLowerCase().includes ("coopernico") ? "Coopernico" : "LuzBoa");

        var costDataset = {
            type: 'bar',
            label: 'Energia ' + supplier,
            data: prices.map(entry => entry.Cost),
            backgroundColor: prices.map((entry, index) => this.getColor(entry.Cost, minCost, oneThird, (index==indexHour))),
            borderColor: prices.map((entry, index) => (index==indexHour) ? 'darkblue' : 'lightgray'),
            borderWidth: 1,
            yAxisID: 'yAxisCost'
        }

        var consumo_vazio_dataset = {
            type: 'line',
            label: 'Vazio',
            data: profileHourly.map(entry => entry.Vazio),
            backgroundColor: 'rgba(84, 234, 9, 0.8)',
            borderWidth: 1,
            yAxisID: 'yAxisConsumo'
        }
        var consumo_cheio_dataset = {
            type: 'line',
            label: 'Cheio',
            data: profileHourly.map(entry => entry.Cheio),
            backgroundColor: 'rgba(15, 225, 225, 0.9)',
            borderWidth: 1,
            yAxisID: 'yAxisConsumo'
        }
        var consumo_ponta_dataset = {
            type: 'line',
            label: 'Ponta',
            data: profileHourly.map(entry => entry.Ponta),
            backgroundColor: 'rgba(15, 120, 225, 0.9)',
            borderWidth: 1,
            yAxisID: 'yAxisConsumo'
        }
        var chartOptions = {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                x: {
                    title: {
                        display: true,
                        text: 'Hora'
                    }
                },
                yAxisConsumo: {
                    position: 'right', 
                    display: false,
                    title: {
                        display: false,
                        text: 'Consumo (kWh)'
                    }                    
                }, 
                yAxisCost: {
                    position: 'left', 
                    title: {
                        display: true,
                        text: 'Custo (€/kWh)'
                    }                    
                }
            },
            plugins: {
                tooltip: {
                    callbacks: {
                        title: function(context) {
                            let title = "Hora: " + context[0].label + ":00";
                            return title;
                        }, 
                        label: function (context) {
                            let label = "";
                            if (context.datasetIndex == 0) {
                                // Custo
                                label += 'Custo: ' + Number(context.parsed.y).toFixed(4) + ' €';
                            }
                            else{
                                // Consumo
                                label += 'Quantidade: ' + Number(context.parsed.y).toFixed(4) + ' kWh';
                            }
                            return label;                            
                        }
                    }
                }              
            }
        };

        var datasets = {}
        if (CONFIG.curva_perfil_c.toLowerCase() === 'true'){
            datasets ={
                labels: prices.map(entry => DateTime.fromISO(entry.Date).toUTC().toFormat('HH')),
                datasets: [costDataset, consumo_vazio_dataset, consumo_cheio_dataset, consumo_ponta_dataset]
            }
        }
        else{
            datasets ={
                labels: prices.map(entry => DateTime.fromISO(entry.Date).toUTC().toFormat('HH')),
                datasets: [costDataset ]
            }
        }

        chart.data = datasets;
        chart.options = chartOptions;   
        chart.update();          
        chart.resize();
    }

    loadDataOnAnalysisChart (chart, data) {
        // Set the chart datasets        
        var consumo_vazio_dataset = {
            type: 'line',
            label: 'Consumo Vazio',
            data: data.map(entry => entry.Vazio),
            backgroundColor: 'rgba(84, 234, 9, 0.8)',
            borderWidth: 1,
            yAxisID: 'yAxisConsumo'
        }
        var consumo_cheio_dataset = {
            type: 'line',
            label: 'Consumo Cheio',
            data: data.map(entry => entry.Cheio),
            backgroundColor: 'rgba(15, 225, 225, 0.9)',
            borderWidth: 1,
            yAxisID: 'yAxisConsumo'
        }
        var consumo_ponta_dataset = {
            type: 'line',
            label: 'Consumo Ponta',
            data: data.map(entry => entry.Ponta),
            backgroundColor: 'rgba(15, 120, 225, 0.9)',
            borderWidth: 1,
            yAxisID: 'yAxisConsumo'
        }
        var costDataset = {
            type: 'bar',
            label: 'Custo',
            data: data.map(entry => entry.Cost),
            backgroundColor:'rgba(241, 163, 6, 0.5)',
            borderColor: 'rgba(241, 163, 6, 0.5)',
            borderWidth: 1,
            yAxisID: 'yAxisCost'
        }

        const panStatus = () => zoomOptions.pan.enabled ? 'enabled' : 'disabled';
        const zoomStatus = (chart) => (zoomOptions.zoom.wheel.enabled ? 'enabled' : 'disabled') + ' (' + chart.getZoomLevel() + 'x)';

        const zoomOptions = {
            pan: {
                enabled: true,
                mode: 'x',
            },
            zoom: {
                wheel: {
                    enabled: true,
                },
                pinch: {
                    enabled: true
                },
                drag: {
                    enabled: false
                },              
                mode: 'x'
            }
        };

        var chartOptions = {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                yAxisConsumo: {
                    position: 'left', 
                    title: {
                        display: true,
                        text: 'Consumo (kWh)'
                    }                    
                }, 
                yAxisCost: {
                    position: 'right', 
                    title: {
                        display: true,
                        text: 'Custo (€/kWh)'
                    }                    
                }
            },
            plugins: {
                zoom : zoomOptions,
                title: {
                    display: true,
                    position: 'bottom',
                    text: (ctx) => 'Zoom: ' + zoomStatus(ctx.chart) + ', Pan: ' + panStatus()
                },
                tooltip: {
                    callbacks: {
                        label: function (context) {
                            let label='';
                            if (context.datasetIndex == 3) {
                                // Custo
                                label = 'Custo: ' + Number(context.parsed.y).toFixed(4) + ' €';
                            }
                            else{
                                // Consumo
                                label = 'Quantidade: ' + Number(context.parsed.y).toFixed(4) + ' kWh';
                            }
                            return label;                            
                        }
                    }
                }              
            }
        };

        var datasets ={
            labels: data.map(entry => entry.Date),
            datasets: [consumo_vazio_dataset, consumo_cheio_dataset, consumo_ponta_dataset, costDataset]
        }

        chart.data = datasets;
        chart.options = chartOptions;   
        chart.update();          
        chart.resize();
    }

    setupSliderForData (chart, data){
        const minmaxdate = this.getMinMaxDate (data);
        const len_data = data.length -1;

        // Save the chart
        this.analysisChart = chart;

        // Save the last data fetched
        this.lastRealDataFetch = data
        this.lastRealDataDisplay = data

        // set label for Periodo
        var min_slide= 0
        var max_slide= len_data
        document.getElementById('analysis-slider-periodo').textContent = data[min_slide].Date + '    -    ' + data[max_slide].Date;

        // configure Slider
        const options ={
            id:'slider_chart',
            enabled: true,
            tooltip: 'hide',
            tooltip_split: false,
            min: min_slide,
            max: max_slide,
            step: 1,
            value: [0, len_data]
        }
        const elem = slider.getElement();
        slider.destroy();
        slider = new Slider('#analysis-data-range', options );
        slider.enable ();

        // Update the chart
        const slider_data = this.lastRealDataDisplay.slice(0, len_data+1)
        this.updateChartAnalysis(chart, slider_data);

        // Update the chart when slider is moved
        slider.on('slideStop', this.onSlideStop );
        slider.on('change', this.onSlideChange );
    }

    onSlideChange(evtValue){
        const value = evtValue.newValue
        if (Array.isArray(value)) {
            const data = priceTrackerApp.lastRealDataFetch
            var min_slide= value[0]
            var max_slide= value[1]
            document.getElementById('analysis-slider-periodo').textContent = data[min_slide].Date + '    -    ' + data[max_slide].Date;
        }                
    }

    onSlideStop (currentValue){
        // Get the data for the slider    
        const slider_data = priceTrackerApp.lastRealDataFetch.slice(currentValue[0], currentValue[1]+1)

        // Update the chart with new period
        priceTrackerApp.updateChartAnalysis( priceTrackerApp.analysisChart, slider_data);

        document.getElementById('title_profile').textContent = "Curva Perfil " + CONFIG.profile + " ..... Em Processamento ...."
        logger ("SLIDING", currentValue, slider_data )
    }

    getCurrentDataForSlider (){
        var data = this.lastRealDataFetch
        var minmaxdate = priceTrackerApp.getMinMaxDate (data)
        return [data, priceTrackerApp.formatDate(minmaxdate.minDate, true), priceTrackerApp.formatDate(minmaxdate.maxDate, true)]
    }

    fetchProfileDataBasedOnManual(minmaxdate, total_vazio, total_cheio, total_ponta ) {
        // Get manual profile data for "Perfil C"
        const lagHour = 0
        apiService.getEstimationProfileManual( this.formatDate(minmaxdate.minDate) , this.formatDate(minmaxdate.maxDate), total_vazio, total_cheio, total_ponta, lagHour, 'json' ).then(response => {
            const profile_average_price_total = response['Total_Cost']/ response['Total_energy'] ;

            document.getElementById('analysis-energia-profile-vazio-total').textContent = `${response['Energy_Vazio'].toFixed(2)} ` + ' kWh' + ' (' + `${(response['Cost_Vazio']).toFixed(2)} ` + ' €)';
            document.getElementById('analysis-energia-profile-cheio-total').textContent = `${response['Energy_Cheio'].toFixed(2)} ` + ' kWh' + ' (' + `${(response['Cost_Cheio']).toFixed(2)} ` + ' €)';
            document.getElementById('analysis-energia-profile-ponta-total').textContent = `${response['Energy_Ponta'].toFixed(2)} ` + ' kWh' + ' (' + `${(response['Cost_Ponta']).toFixed(2)} ` + ' €)';
            document.getElementById('analysis-energia-profile-total').textContent       = `${response['Total_energy'].toFixed(2)} ` + ' kWh' ;
            document.getElementById('analysis-custo-profile-total').textContent         = `${response['Total_Cost'].toFixed(2)} ` + ' €';
            document.getElementById('analysis-custo-profile-preco-medio').textContent   = `${profile_average_price_total.toFixed(6)} ` + ' €/kWh';

            document.getElementById('analysis-custo-profile-preco-medio-vazio').textContent = `${response['Vazio_avg_price_cost'].toFixed(6)} ` + ' €/kWh';
            document.getElementById('analysis-custo-profile-preco-medio-cheio').textContent = `${response['Cheio_avg_price_cost'].toFixed(6)} ` + ' €/kWh';
            document.getElementById('analysis-custo-profile-preco-medio-ponta').textContent = `${response['Ponta_avg_price_cost'].toFixed(6)} ` + ' €/kWh';

            if (response['records'] != ''){
                const data = response['records'];
                this.lastEstimateDataFetch = data 
            }

            const switchChartReal = document.getElementById('switch-chart').checked
            if (!switchChartReal && this.lastEstimateDataFetch !== null) {
                priceTrackerApp.loadDataOnAnalysisChart (priceChartAnalysis, priceTrackerApp.lastEstimateDataFetch);
            }
            
            if (CONFIG.supplier == "luzboa-spot") {
                document.getElementById('title_profile').textContent = "Cálculo do Consumo :"
            }
            else{
                document.getElementById('title_profile').textContent = "Curva Perfil " + CONFIG.profile
            }
            
        })
        .catch(error => {
            console.error('fetchProfileDataBasedOnManual Error:', error);
        });
    }
   
    fetchProfileDataBasedOnFile(minmaxdate, total_energy ) {
        // Get the file data from the form
        apiService.getEstimationProfile( this.formatDate(minmaxdate.minDate), this.formatDate(minmaxdate.maxDate), total_energy, 'json' ).then(response => {
            const profile_average_price_total = response['Total_Cost']/ response['Total_energy'] ;

            document.getElementById('analysis-energia-profile-vazio-total').textContent = `${response['Energy_Vazio'].toFixed(2)} ` + ' kWh' + ' (' + `${(response['Cost_Vazio']).toFixed(2)} ` + ' €)';
            document.getElementById('analysis-energia-profile-cheio-total').textContent = `${response['Energy_Cheio'].toFixed(2)} ` + ' kWh' + ' (' + `${(response['Cost_Cheio']).toFixed(2)} ` + ' €)';
            document.getElementById('analysis-energia-profile-ponta-total').textContent = `${response['Energy_Ponta'].toFixed(2)} ` + ' kWh' + ' (' + `${(response['Cost_Ponta']).toFixed(2)} ` + ' €)';
            document.getElementById('analysis-energia-profile-total').textContent       = `${response['Total_energy'].toFixed(2)} ` + ' kWh' ;
            document.getElementById('analysis-custo-profile-total').textContent         = `${response['Total_Cost'].toFixed(2)} ` + ' €';
            document.getElementById('analysis-custo-profile-preco-medio').textContent   = `${profile_average_price_total.toFixed(6)} ` + ' €/kWh';

            document.getElementById('analysis-custo-profile-preco-medio-vazio').textContent = `${response['Vazio_avg_price_cost'].toFixed(4)} ` + ' €/kWh';
            document.getElementById('analysis-custo-profile-preco-medio-cheio').textContent = `${response['Cheio_avg_price_cost'].toFixed(4)} ` + ' €/kWh';
            document.getElementById('analysis-custo-profile-preco-medio-ponta').textContent = `${response['Ponta_avg_price_cost'].toFixed(4)} ` + ' €/kWh';


            if (response['records'] != ''){
                const data = response['records'];
                this.lastEstimateDataFetch = data
                this.lastEstimateDataDisplay = this.lastEstimateDataFetch;
            }

            const switchChartReal = document.getElementById('switch-chart').checked
            if (switchChartReal == false) {
                this.isRealConsumption = false;
                priceTrackerApp.loadDataOnAnalysisChart (priceChartAnalysis, priceTrackerApp.lastEstimateDataFetch);
            }

            document.getElementById('title_profile').textContent = "Curva Perfil " + CONFIG.profile
        })
        .catch(error => {
            console.error('fetchProfileDataBasedOnFile Error:', error);
        });
    }

} 


// Initialize the main variables for the web app
var apiService = null;
var priceTrackerApp = null;
var priceChart, priceChartNet, priceChartAnalysis = null;
var slider = null;
var timerId;
var file = null;
var sample = '';


// On Start
document.addEventListener('DOMContentLoaded', () => {

    const savedConfig = localStorage.getItem('config');
    if (savedConfig) {
        const config = JSON.parse(savedConfig);
        document.getElementById('config-perfil').value = (config.curva_perfil_c=== undefined)? CONFIG.curva_perfil_c : config.curva_perfil_c;
        document.getElementById('config-hours').value = (config.hours === undefined) ? CONFIG.chart_update_period : config.hours;
        document.getElementById('config-tariff').value = (config.tariff === undefined)? CONFIG.tariff : config.tariff;
        document.getElementById('config-supplier').value = (config.supplier=== undefined)? CONFIG.supplier : config.supplier;
        document.getElementById('config-year').value = (config.year=== undefined)? CONFIG.year : config.year ;
        document.getElementById('config-eot-key').value = (config.configEotKey === undefined) ? CONFIG.EoTKey : config.configEotKey;
        document.getElementById('config-cycle-day').value = (config.cycle_day === undefined) ? CONFIG.cycle_day : config.cycle_day;
        document.getElementById('config-profile').value = (config.profile === undefined) ? CONFIG.profile : config.profile;
        document.getElementById('config-refresh-rate').value = (config.chart_update_interval === undefined) ? CONFIG.chart_update_interval : config.chart_update_interval;

        CONFIG.chart_update_interval = (config.chart_update_interval === undefined) ? CONFIG.chart_update_interval : config.chart_update_interval;
        CONFIG.chart_update_period = (config.hours === undefined) ? CONFIG.chart_update_period : config.hours;
        CONFIG.chart_update_period_net = (config.hours === undefined) ? CONFIG.chart_update_period_net : config.hours;
        CONFIG.tariff =  (config.tariff === undefined)? CONFIG.tariff : config.tariff;
        CONFIG.supplier = (config.supplier=== undefined)? CONFIG.supplier : config.supplier;
        CONFIG.year = (config.year=== undefined)? CONFIG.year : config.year ;
        CONFIG.EoTKey = (config.configEotKey === undefined) ? CONFIG.EoTKey : config.configEotKey;
        CONFIG.curva_perfil_c = (config.curva_perfil_c=== undefined)? CONFIG.curva_perfil_c : config.curva_perfil_c;
        CONFIG.cycle_day = (config.cycle_day === undefined) ? CONFIG.cycle_day : config.cycle_day;
        CONFIG.profile = (config.profile === undefined) ? CONFIG.profile : config.profile;
    
        logger ('Loaded CONFIG: ', CONFIG);            
    }

    const address =  window.location.href;
    const protocol = window.location.protocol;
    const hostname = window.location.hostname;
    const port = ':5000';
    const pathname = window.location.pathname;    
    const newURL = `${protocol}//${hostname}${port}${pathname}`;

    if (hostname === 'localhost' || window.location.protocol === 'file:') {
        CONFIG.api_url = 'http://localhost' + port;
        CONFIG.allowDebug = true;
    } else {
        CONFIG.api_url = `${protocol}//${hostname}${port}`;
        CONFIG.allowDebug = false;
    }
    logger ('Starting the app... ADDRESS:',  CONFIG.api_url );
    
    apiService = new ApiService(CONFIG.api_url);
    priceTrackerApp = new PriceTracker(apiService, CONFIG.chart_update_period);
    [priceChart, priceChartNet, priceChartAnalysis] = priceTrackerApp.initCharts();    

    // Add event listeners to the Analise de Consumo button
    const consumoBrowseFile = document.getElementById("consumo-browse-file");
    consumoBrowseFile.addEventListener("change", handleFileInputChange);

    const contador_refresh = document.getElementById("analysis-contador-refresh");
    contador_refresh.addEventListener("click", handleAnaliseRefreshContador);

    const switchCheckboxChart = document.getElementById('switch-chart');
    switchCheckboxChart.addEventListener('change', handleSwitchChange_Chart);

    const configForm = document.getElementById('configForm');
    configForm.addEventListener('submit', handleStoreConfig);

    // Set correct title
    document.getElementById('title_profile').textContent = "Curva Perfil " + CONFIG.profile

    // Configure the slider
    slider = new Slider('#analysis-data-range', {enabled: false});

    // Configure the Analysis Page according to the Supplier
    priceTrackerApp.setAnalysisPageforSupplier(CONFIG.supplier, CONFIG.tariff)

    // Call the function immediately
    priceTrackerApp.fetchServerData( priceChart, priceChartNet, force=true);

    // Call the function resetTimer to enable the timer
    resetTimer ();

    // Add the click event listener to the Analysis Chart Toolbar.
    document.getElementById("btn-reset-zoom").addEventListener("click", function() {
        priceChartAnalysis.resetZoom ();
    });
    document.getElementById("btn-zoom-in").addEventListener("click", function() {
        priceChartAnalysis.zoom ({x: 1.1});
    });
    document.getElementById("btn-zoom-out").addEventListener("click", function() {
        priceChartAnalysis.zoom ({x: 0.9});
    });    
    document.getElementById("btn-pan-right").addEventListener("click", function() {
        priceChartAnalysis.pan ({x: -100}, undefined, 'default');
    });
    document.getElementById("btn-pan-left").addEventListener("click", function() {
        priceChartAnalysis.pan ({x: 100}, undefined, 'default');
    });
    
    document.getElementById("btn-resample-15m").addEventListener("click", function() {
        sample = '';
        
        if (file) {
            uploadFile (file)
        }        
    });    
    document.getElementById("btn-resample-1h").addEventListener("click", function() {
        sample = '1H';
        if (file) {
            uploadFile (file)
        }        
    });
    document.getElementById("btn-resample-1d").addEventListener("click", function() {
        sample = '1D';
        if (file) {
            uploadFile (file)
        }        
    });
    document.getElementById("btn-resample-1w").addEventListener("click", function() {
        sample = '1W';
        if (file) {
            uploadFile (file)
        }        
    });    

    const inputIds = ['analysis-contador-vazio', 'analysis-contador-cheio', 'analysis-contador-ponta'];
    for (let id of inputIds) {
        const input = document.getElementById(id);
        input.addEventListener("change", function() {
            validateInput(input);
        });
    }

    const now = new Date();
    const start_date  = new Date(now.getFullYear(), now.getMonth(), now.getDate() - 2, 0, 0);
    const end_date = new Date(now.getFullYear(), now.getMonth(), now.getDate() - 1, 23, 59);

    const dtp_start = document.getElementById('analysis-periodo-start');
    flatpickr(dtp_start, {
        enableTime: true,
        dateFormat: "Y-m-d H:i",
        defaultDate: start_date,
        time_24hr: true
      });

    const dtp_end = document.getElementById('analysis-periodo-end');
    flatpickr(dtp_end, {
        enableTime: true,
        dateFormat: "Y-m-d H:i",
        defaultDate: end_date,
        time_24hr: true
    });
  

});

// ********** Functions for the management of the UI **********
// Custom log function
function logger(...args) {
    var debug = false
    const savedConfig = localStorage.getItem('config');
    if (savedConfig) {
        const config = JSON.parse(savedConfig);
        debug = (config.debug === undefined) ? CONFIG.allowDebug : config.debug;
    }

    if (debug) {
      console.log(...args);
    }
}

function resetTimer() {
  // Clear the existing timer
  if (timerId) {
    clearInterval(timerId);
  }

  // Set the new timer with the updated interval
  timerId = setInterval(() => {
    priceTrackerApp.fetchServerData(priceChart, priceChartNet);
  }, 60000 * CONFIG.chart_update_interval);
}

function handleStoreConfig (event) {
    event.preventDefault();

    const hours  = document.getElementById('config-hours').value;
    const curva_perfil_c = document.getElementById('config-perfil').value;
    const tariff = document.getElementById('config-tariff').value;
    const supplier = document.getElementById('config-supplier').value;
    const year   = document.getElementById('config-year').value;
    const configEotKey = document.getElementById('config-eot-key').value;
    const cycle_day = document.getElementById('config-cycle-day').value;
    const profile = document.getElementById('config-profile').value;
    const chart_update_interval = document.getElementById('config-refresh-rate').value;

    const apiUrl = CONFIG.api_url;

    const config = {
        apiUrl,
        hours,
        tariff,
        year,
        configEotKey,
        supplier,
        curva_perfil_c,
        cycle_day,
        profile,
        chart_update_interval
    };

    CONFIG.chart_update_period = hours;
    CONFIG.chart_update_period_net = hours;
    CONFIG.tariff = tariff;
    CONFIG.supplier = supplier;
    CONFIG.year = year;
    CONFIG.EoTKey = configEotKey;
    CONFIG.curva_perfil_c = curva_perfil_c;
    CONFIG.cycle_day = cycle_day;
    CONFIG.profile = profile;
    CONFIG.chart_update_interval = chart_update_interval;
    
    priceTrackerApp.apiService = new ApiService(apiUrl);
    priceTrackerApp.chartUpdatePeriod = hours;
    priceTrackerApp.chartUpdatePeriodNet = hours;

    localStorage.setItem('config', JSON.stringify(config));
    logger('Configuration saved!', CONFIG);

    // Call the function resetTimer to enable the timer
    resetTimer ();

    // Configure the Analysis Page according to the Supplier
    priceTrackerApp.setAnalysisPageforSupplier(CONFIG.supplier, CONFIG.tariff)

    // Call the Fetch function to updathe charts
    priceTrackerApp.fetchServerData( priceChart, priceChartNet, force=true);

    const inicioTab = new bootstrap.Tab(document.getElementById('home-tab'));
    inicioTab.show();    
}

function handleSwitchChange_Chart(event){
    if (event.target.checked) {
        if (priceTrackerApp.lastRealDataFetch == null) {
            // No data to process
            return;
        }
        // Show the chart using the Real Energy Data
        priceTrackerApp.isRealConsumption = true
        priceTrackerApp.loadDataOnAnalysisChart (priceChartAnalysis, priceTrackerApp.lastRealDataDisplay );  // priceTrackerApp.lastRealDataFetch);
    } else {
        if (priceTrackerApp.lastEstimateDataFetch == null) {
            // No data to process
            return;
        }    
        // Show the chart using the Estimate Energy Data
        priceTrackerApp.isRealConsumption = false
        priceTrackerApp.loadDataOnAnalysisChart (priceChartAnalysis, priceTrackerApp.lastEstimateDataFetch); // lastEstimateDataDisplay);  //.lastEstimateDataFetch);
    }
}

function handleAnaliseRefreshContador (event){

    if (CONFIG.supplier == "luzboa-spot") {
        const tarifa = CONFIG.tariff;
        
        // Map the tariff to the input ids needed to validate
        const tarifaMap = {
            simples: ['analysis-contador-cheio'],
            bi: ['analysis-contador-vazio', 'analysis-contador-cheio'],
            tri: ['analysis-contador-vazio', 'analysis-contador-cheio', 'analysis-contador-ponta']
        };
        
        // Find the tariff key
        const tarifaKey = Object.keys(tarifaMap).find(key => tarifa.toLowerCase().includes(key));
        // Get the input ids
        const ids = tarifaMap[tarifaKey] || [];

        const form = document.getElementById('analysisForm');
        const inputsToValidate = document.querySelectorAll('[data-validate]');
        let isValid = true;
      
        inputsToValidate.forEach(input => {
            if (ids.includes(input.id)) {
              const value = parseFloat(input.value);
              if (isNaN(value) || value <= 0) {
                input.classList.add('is-invalid');
                isValid = false;
              } else {
                input.classList.remove('is-invalid');
              }
            }
        });

        form.classList.add('was-validated');

        if (isValid) {
            const dtp_start = new Date (document.getElementById('analysis-periodo-start').value);
            const dtp_end = new Date(document.getElementById('analysis-periodo-end').value) ;

            const period = { minDate: dtp_start, maxDate: dtp_end }

            var inputVazio = ( document.getElementById('analysis-contador-vazio').value );
            var inputCheio = ( document.getElementById('analysis-contador-cheio').value );
            var inputPonta = ( document.getElementById('analysis-contador-ponta').value ) ;

            document.getElementById('title_profile').textContent = "Cálculo do Consumo :" + " ..... Em Processamento ...."
            priceTrackerApp.fetchProfileDataBasedOnManual(period, inputVazio, inputCheio, inputPonta,)
        }

        return;
    }

    // In case of Coopernico supplier:
    if (priceTrackerApp.lastRealDataFetch == null) {
        // No data to process
        return;
    }

    document.getElementById('title_profile').textContent = "Curva Perfil " + CONFIG.profile + " ..... Em Processamento ...."
    const minmaxdate = priceTrackerApp.getMinMaxDate (priceTrackerApp.lastRealDataDisplay);
    const totalEnergyAndCost = priceTrackerApp.getTotalEnergyCost(priceTrackerApp.lastRealDataDisplay);

    var inputVazio = ( document.getElementById('analysis-contador-vazio').value );
    var inputCheio = ( document.getElementById('analysis-contador-cheio').value );
    var inputPonta = ( document.getElementById('analysis-contador-ponta').value ) ;

    if (inputVazio == 0){
        inputVazio = totalEnergyAndCost.sumVazio.toFixed(2) ;
        document.getElementById('analysis-contador-vazio').value = inputVazio;
    }
    if (inputCheio == 0){
        inputCheio = totalEnergyAndCost.sumCheio.toFixed(2) ;
        document.getElementById('analysis-contador-cheio').value = inputCheio;
    }
    if (inputPonta == 0){
        inputPonta = totalEnergyAndCost.sumPonta.toFixed(2) ;
        document.getElementById('analysis-contador-ponta').value = inputPonta;
    }

    priceTrackerApp.fetchProfileDataBasedOnManual(minmaxdate, inputVazio, inputCheio, inputPonta,)
}

async function handleFileInputChange(event) {
    if (!event.target.files || event.target.files.length === 0) {
        return;
    }

    file = event.target.files[0];
    if (file) {
        uploadFile (file)
    }
}

async function uploadFile (file){
    if (file) {
        try {
            document.getElementById('title_consumo_real').textContent = "Consumo Real : ..... Em Processamento ....";
            showAlert('Aguarde enquanto os dados são processados...');

            const provider = document.getElementById("analisys-provider").innerHTML.trim();
            logger('File selected:', file.name, 'Provider:', provider);

            const response = await apiService.uploadEnergyFile('json', provider, file);
            logger('UploadEnergyFile: File uploaded successfully:', response);

            // Turn all switchs On
            // document.getElementById('switch-dados-perfil-c').checked = false;
            document.getElementById('switch-chart').checked = true;

            // Setup Slider
            priceTrackerApp.setupSliderForData(priceChartAnalysis, response);
            hideAlert();
        } catch (error) {
            showAlert('Erro no processamento do ficheiro. Verifique se está a usar o ficheiro correto...');
            console.error('Error uploading file:', error);
        }
    }

}

function updateDropDownProviderTitle(event, option) {
    // Update the Tariff dropdown menu title
    event.preventDefault();
    const dropdownMenuButton = document.getElementById("analisys-provider");
    dropdownMenuButton.innerHTML = option.innerHTML;
}

function updateDropDownDivisor(event, option) {
    event.preventDefault();
    document.getElementById("analisys-divisor").innerHTML = option.innerHTML;
    if (option.innerHTML == "Auto"){
        CONFIG.divisor = 0;
    }
    else{
        CONFIG.divisor = option.innerHTML;
    }
}

function showAlert(message) {
    const alert = document.getElementById('processingAlert');
    // Update the alert message
    const messageElement = alert.querySelector('strong');
    messageElement.textContent = message;    
    alert.style.display = 'block';
    new bootstrap.Alert(alert);
}
  
function hideAlert() {
    const alert = document.getElementById('processingAlert');
    alert.style.display = 'none';
}

function validateInput(input) {
    const value = parseFloat(input.value);

    if (isNaN(value) || value < 0) {
        input.classList.add("is-invalid");
        return false;
    } else {
        input.classList.remove("is-invalid");
        return true;
    }
}

function updateDropDownTariffTitle(event, option) {
    event.preventDefault();
    const dropdownMenuButton = document.getElementById("current-tarifario");
    dropdownMenuButton.innerHTML = option.innerHTML;
}
