import pandas as pd
import utils as ut
import dgr_consolidate as dgr
from sqlalchemy import create_engine
import warnings
warnings.filterwarnings("ignore")

###############################################

dgr.consolidate()
db_connection = create_engine('mysql+mysqldb://root:lokesh@localhost/')

query = "SELECT * FROM generation_mastersheet"
master_sheet = pd.read_sql(query, con=db_connection)

result_path = 'results/'                 

generation_forecast = pd.DataFrame()
breakdown_forecast = pd.DataFrame()

#################################################################

for loc_key in set(master_sheet.reset_index()['wind_turbine_location_number']):
    data = master_sheet[master_sheet['wind_turbine_location_number'] == loc_key]
    active_generation = data[['date', 'day_generation_kwh']]
    breakdown_data = data[['date', 'grid_failure', 'force_majeure', 'scheduled_services', 'unscheduled_services',
                           'internal_grid_failure_hours', 'nor']]

    if active_generation.shape[0] <= 365:
        continue
        # raise ValueError('Not Sufficient dataset to forecast.')

    if breakdown_data.shape[0] <= 365:
        continue
        # raise ValueError('Not Sufficient dataset to forecast.')

    print(loc_key)
    print('---------------')

    ###############################################

    active_generation = active_generation.reset_index().rename(columns={'day_generation_kwh': 'Generation'})
    active_generation.set_index('date', inplace=True)
    active_generation.set_index(pd.to_datetime(active_generation.index), inplace=True)
    active_generation['Generation'].fillna(0.0, inplace=True)
    active_generation = active_generation['Generation'].resample('MS').sum()
    active_generation.sort_index(inplace=True)
    active_generation = active_generation.reset_index()

    breakdown_data['date'] = pd.to_datetime(breakdown_data['date'])
    breakdown_data = breakdown_data.reset_index().rename(columns={'grid_failure': 'GF', 'force_majeure': 'FM',
                                                                  'scheduled_services': 'S', 'unscheduled_services': 'U',
                                                                  'internal_grid_failure_hours': 'IGF', 'nor': 'NOR'})

    breakdown_GF = ut.extract_data(breakdown_data, 'GF')

    breakdown_FM = ut.extract_data(breakdown_data, 'FM')

    breakdown_S = ut.extract_data(breakdown_data, 'S')

    breakdown_U = ut.extract_data(breakdown_data, 'U')

    breakdown_IGF = ut.extract_data(breakdown_data, 'IGF')

    breakdown_NOR = ut.extract_data(breakdown_data, 'NOR')

    ################################################

    gen_forecast = ut.randomForest(active_generation, 'Generation', 24)

    info_frame = pd.DataFrame()
    info_frame['date'] = gen_forecast['date']
    info_frame['FY'] = ['FY ' + str(y) + '-' + str(y+1-2000) if x not in [1, 2, 3] else 'FY '+str(y-1)+'-'+str(y-2000) for x, y in
                        zip(info_frame['date'].dt.month, info_frame['date'].dt.year)]
    info_frame['Firm'] = [data.loc[data.index[0], 'customer_name']] * len(info_frame)
    info_frame['OEM'] = [data.loc[data.index[0], 'client_name']] * len(info_frame)
    info_frame['State'] = [data.loc[data.index[0], 'state']] * len(info_frame)
    info_frame['Site'] = [data.loc[data.index[0], 'site_name']] * len(info_frame)
    info_frame['WTG No.'] = [data.loc[data.index[0], 'wind_turbine_location_number']] * len(info_frame)

    generation = pd.merge(info_frame, gen_forecast, on='date')
    generation_forecast = pd.concat([generation_forecast, generation], ignore_index=True)

    ################################################

    GF_forecast = ut.randomForest(breakdown_GF, 'GF', 730)
    GF_forecast['GF Forecast RoundUp'] = GF_forecast['GF Forecast'].round()

    FM_forecast = ut.randomForest(breakdown_FM, 'FM', 730)
    FM_forecast['FM Forecast RoundUp'] = FM_forecast['FM Forecast'].round()

    S_forecast = ut.randomForest(breakdown_S, 'S', 730)
    S_forecast['S Forecast RoundUp'] = S_forecast['S Forecast'].round()

    U_forecast = ut.randomForest(breakdown_U, 'U', 730)
    U_forecast['U Forecast RoundUp'] = U_forecast['U Forecast'].round()

    IGF_forecast = ut.randomForest(breakdown_IGF, 'IGF', 730)
    IGF_forecast['IGF Forecast RoundUp'] = IGF_forecast['IGF Forecast'].round()

    NOR_forecast = ut.randomForest(breakdown_NOR, 'NOR', 730)
    NOR_forecast['NOR Forecast RoundUp'] = NOR_forecast['NOR Forecast'].round()

    info_frame = pd.DataFrame()
    info_frame['date'] = GF_forecast['date']
    info_frame['FY'] = ['FY ' + str(y) + '-' + str(y+1-2000) if x not in [1, 2, 3] else 'FY '+str(y-1)+'-'+str(y-2000) for x, y in
                        zip(info_frame['date'].dt.month, info_frame['date'].dt.year)]
    info_frame['Firm'] = [data.loc[data.index[0], 'customer_name']] * len(info_frame)
    info_frame['OEM'] = [data.loc[data.index[0], 'client_name']] * len(info_frame)
    info_frame['State'] = [data.loc[data.index[0], 'state']] * len(info_frame)
    info_frame['Site'] = [data.loc[data.index[0], 'site_name']] * len(info_frame)
    info_frame['WTG No.'] = [data.loc[data.index[0], 'wind_turbine_location_number']] * len(info_frame)

    breakdown = pd.merge(info_frame, GF_forecast, on='date').merge(FM_forecast, on='date').merge(S_forecast, on='date').merge(U_forecast, on='date').merge(IGF_forecast, on='date').merge(NOR_forecast, on='date')
    breakdown_forecast = pd.concat([breakdown_forecast, breakdown], ignore_index=True)

generation_forecast = generation_forecast.reset_index().rename(columns={'date': 'Date'})
breakdown_forecast = breakdown_forecast.reset_index().rename(columns={'date': 'Date'})
generation_forecast = generation_forecast.drop(columns=['index'])
breakdown_forecast = breakdown_forecast.drop(columns=['index'])

# ut.save_excel(generation_forecast, 'Forecast', 'Generation', result_path)
# ut.save_excel(breakdown_forecast, 'Forecast', 'Breakdown', result_path)

generation_forecast.to_sql(con=db_connection, name='generation_forecast', if_exists='replace', index=False)
breakdown_forecast.to_sql(con=db_connection, name='breakdown_forecast', if_exists='replace', index=False)
