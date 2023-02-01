import pandas as pd
import os
import old_utils as ut
import warnings
warnings.filterwarnings("ignore")

###############################################

input_path = 'input_data/'
result_path = 'results/'

input_excel_file = [(name[0], filename) for name in os.walk(input_path) for filename in name[2] if filename.endswith('.xlsx')]

if len(input_excel_file) != 1:
    raise ValueError('Please check input folder. Might be there are not any excel file or more than one excel file.')

###############################################

active_generation_all = pd.read_excel(os.path.join(input_excel_file[0][0], input_excel_file[0][1]), sheet_name='Generation')
wind_speed_all = pd.read_excel(os.path.join(input_excel_file[0][0], input_excel_file[0][1]), sheet_name='Wind_Speed')
breakdown_all = pd.read_excel(os.path.join(input_excel_file[0][0], input_excel_file[0][1]), sheet_name='Breakdown')

###############################################

for loc_key in set(active_generation_all.reset_index()['Loc. No.']):

    active_generation = active_generation_all[active_generation_all['Loc. No.'] == loc_key]
    wind_speed = wind_speed_all[wind_speed_all['Turbine'] == loc_key]
    breakdown_data = breakdown_all[breakdown_all['Loc. No.'] == loc_key]

    if active_generation.shape[0] <= 365:
        raise ValueError('Not Sufficient dataset to forecast.')

    if wind_speed.shape[0] <= 12:
        raise ValueError('Not Sufficient dataset to forecast.')

    if breakdown_data.shape[0] <= 365:
        raise ValueError('Not Sufficient dataset to forecast.')

    if not os.path.exists(result_path + loc_key):
        os.mkdir(os.path.join(result_path, loc_key))

    ###############################################

    active_generation = active_generation.reset_index().rename(columns={'Daily Gen.(kWh)': 'Generation'})
    active_generation.set_index('Date', inplace=True)
    active_generation.set_index(pd.to_datetime(active_generation.index), inplace=True)
    active_generation = active_generation['Generation'].resample('MS').sum()
    active_generation = active_generation.fillna(' ')
    active_generation = active_generation.reset_index()

    wind_speed = wind_speed.reset_index().rename(columns={'period': 'Date', 'Wind\nSpeed\n(m/s)': 'WindSpeed'})
    wind_speed.set_index('Date', inplace=True)
    wind_speed.set_index(pd.to_datetime(wind_speed.index), inplace=True)
    wind_speed = wind_speed['WindSpeed']
    wind_speed = wind_speed.fillna(' ')
    wind_speed = wind_speed.reset_index()

    breakdown_data = breakdown_data.rename(columns={'Gen. Date': 'Date'})
    breakdown_data['Date'] = pd.to_datetime(breakdown_data['Date'])

    breakdown_GF = ut.extract_data(breakdown_data, 'GF')

    breakdown_FM = ut.extract_data(breakdown_data, 'FM')

    breakdown_S = ut.extract_data(breakdown_data, 'S')

    breakdown_U = ut.extract_data(breakdown_data, 'U')

    ################################################

    forecast_gen = ut.randomForest(active_generation, 'Generation', 24)
    ut.save_plots(forecast_gen, 'Generation', 24, loc_key, result_path + loc_key + '/')

    forecast_wind = ut.randomForest(wind_speed, 'WindSpeed', 24)
    ut.save_plots(forecast_wind, 'WindSpeed', 24, loc_key, result_path + loc_key + '/')

    generation_wind_forecast = pd.merge(forecast_gen, forecast_wind, on="Date")
    ut.save_excel(generation_wind_forecast, 'Generation & Wind Speed', loc_key, result_path + loc_key + '/')

    ################################################

    forecast_GF = ut.randomForest(breakdown_GF, 'GF', 730)
    ut.save_plots(forecast_GF, 'GF', 730, loc_key, result_path + loc_key + '/')

    forecast_FM = ut.randomForest(breakdown_FM, 'FM', 730)
    ut.save_plots(forecast_FM, 'FM', 730, loc_key, result_path + loc_key + '/')

    forecast_S = ut.randomForest(breakdown_S, 'S', 730)
    ut.save_plots(forecast_S, 'S', 730, loc_key, result_path + loc_key + '/')

    forecast_U = ut.randomForest(breakdown_U, 'U', 730)
    ut.save_plots(forecast_U, 'U', 730, loc_key, result_path + loc_key + '/')

    info_frame = pd.DataFrame()
    info_frame['Date'] = forecast_GF['Date']
    info_frame['Loc. No.'] = [breakdown_data.loc[0, 'Loc. No.']] * len(info_frame)
    info_frame['Customer Name'] = [breakdown_data.loc[0, 'Customer Name']] * len(info_frame)
    info_frame['State'] = [breakdown_data.loc[0, 'State']] * len(info_frame)
    info_frame['Site'] = [breakdown_data.loc[0, 'Site']] * len(info_frame)

    breakdown_forecast = pd.merge(info_frame, forecast_GF, on="Date").merge(forecast_FM, on="Date").merge(forecast_S, on="Date").merge(forecast_U, on="Date")
    ut.save_excel(breakdown_forecast, 'Breakdowns', loc_key, result_path + loc_key + '/')
