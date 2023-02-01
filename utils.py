import calendar
from datetime import timedelta
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import accuracy_score
import warnings
warnings.filterwarnings("ignore")


def get_factor(factor):
    if factor == 'Generation':
        return True
    return False


def extract_data(dataframe, factor):
    dataframe.fillna(0.0, inplace=True)
    dataframe = dataframe[['date', factor]]
    dataframe['Probability'] = [1.0 if x > 0 else 0.0 for x in dataframe[factor]]
    dataframe = dataframe.drop([factor], axis=1)
    dataframe = dataframe.rename(columns={'Probability': factor})
    dataframe.sort_values('date', inplace=True)
    return dataframe


def add_dates(dataframe, factor, forecast_length):
    end_point = len(dataframe)
    df = pd.DataFrame(index=range(forecast_length), columns=range(2))
    df.columns = ['date', factor]
    dataframe = dataframe.append(df)
    dataframe = dataframe.reset_index(drop=True)

    x = dataframe.at[end_point - 1, 'date']
    x = pd.to_datetime(x, format='%Y-%m-%d')

    if get_factor(factor):
        for i in range(forecast_length):
            days_in_month = calendar.monthrange(x.year, x.month)[1]
            x = dataframe.at[dataframe.index[end_point + i], 'date'] = x + timedelta(days=days_in_month)
    else:
        for i in range(forecast_length):
            dataframe.at[dataframe.index[end_point + i], 'date'] = x + timedelta(days=i+1)

    dataframe['date'] = pd.to_datetime(dataframe['date'], format='%Y-%m-%d')
    dataframe['Month'] = dataframe['date'].dt.month
    dataframe['Day'] = dataframe['date'].dt.day

    return dataframe


def find_accuracy(rfr, train_x, train_y, factor):
    prediction = rfr.predict(train_x)

    print(factor)

    if get_factor(factor):
        print('Accuracy:', (rfr.score(train_x, train_y)*100).round(2))
    else:
        print('Accuracy:', (accuracy_score(train_y, prediction.round()) * 100).__round__(2))

    print('---------------')


def randomForest(dataframe, factor, forecast_length):
    new_dataframe = add_dates(dataframe, factor, forecast_length)
    new_dataframe = new_dataframe.reset_index(drop=True)

    end_point = len(dataframe)
    train = new_dataframe.loc[:end_point - 1, :]
    train_x = train[['Month', 'Day']]
    train_y = train[factor]

    rfr = RandomForestRegressor(n_estimators=75, random_state=1)
    rfr.fit(train_x, train_y)

    # noinspection PyTypeChecker
    find_accuracy(rfr, train_x, train_y, factor)

    forecast_values = []
    input_data = new_dataframe.loc[end_point:, ~new_dataframe.columns.isin(['date', factor])]
    prediction = rfr.predict(input_data)

    for i in range(end_point):
        forecast_values.append(np.NAN)
    for i in range(forecast_length):
        forecast_values.append(prediction[i])

    new_dataframe[factor + ' Forecast'] = forecast_values
    new_dataframe = new_dataframe.drop(columns=['Day', 'Month'])

    return new_dataframe


def save_excel(excel_data, sheet_name, loc, folder):
    excel_data['Date'] = excel_data['Date'].dt.date
    excel_data.to_excel(excel_writer=folder + loc + ' ' + sheet_name + '.xlsx', sheet_name=loc + ' ' + sheet_name, index=False)
