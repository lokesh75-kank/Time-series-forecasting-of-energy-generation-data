import calendar
from datetime import timedelta
import matplotlib
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import accuracy_score
import warnings
warnings.filterwarnings("ignore")


def get_factor(factor):
    if factor in ['Generation', 'WindSpeed']:
        return True
    return False


def extract_data(dataframe, factor):
    dataframe = dataframe[['Date', factor]]
    dataframe['Probability'] = [1.0 if x > 0 else 0.0 for x in dataframe[factor]]
    dataframe = dataframe.drop([factor], axis=1)
    dataframe = dataframe.rename(columns={'Probability': factor})
    return dataframe


def add_dates(dataframe, factor, forecast_length):
    end_point = len(dataframe)
    df = pd.DataFrame(index=range(forecast_length), columns=range(2))
    df.columns = ['Date', factor]
    dataframe = dataframe.append(df)
    dataframe = dataframe.reset_index(drop=True)

    x = dataframe.at[end_point - 1, 'Date']
    x = pd.to_datetime(x, format='%Y-%m-%d')

    if get_factor(factor):
        for i in range(forecast_length):
            days_in_month = calendar.monthrange(x.year, x.month)[1]
            x = dataframe.at[dataframe.index[end_point + i], 'Date'] = x + timedelta(days=days_in_month)
    else:
        for i in range(forecast_length):
            dataframe.at[dataframe.index[end_point + i], 'Date'] = x + timedelta(days=i+1)

    dataframe['Date'] = pd.to_datetime(dataframe['Date'], format='%Y-%m-%d')
    dataframe['Month'] = dataframe['Date'].dt.month
    dataframe['Day'] = dataframe['Date'].dt.day

    return dataframe


def find_accuracy(fit, dataframe, new_dataframe, factor, forecast_length):
    df = new_dataframe[['Month', 'Day']]
    prediction = fit.predict(df)

    print(factor)

    if get_factor(factor):
        mape = []
        for x in range(len(dataframe)):
            temp = abs(dataframe.iloc[x][factor] - prediction[x]) / abs(dataframe.iloc[x][factor])
            if temp < float('inf'):
                mape.append(temp)
        mape = np.mean(mape)
        print('Accuracy:', (100 - (mape * 100)).__round__(2))
    else:
        print('Accuracy:', (accuracy_score(dataframe[factor], prediction[:-forecast_length].round()) * 100).__round__(2))

    print('---------------')


def randomForest(dataframe, factor, forecast_length):
    new_dataframe = add_dates(dataframe, factor, forecast_length)
    new_dataframe = new_dataframe.reset_index(drop=True)

    end_point = len(dataframe)
    train = new_dataframe.loc[:end_point - 1, :]
    train_x = train[['Month', 'Day']]
    train_y = train[factor]

    rfr = RandomForestRegressor(n_estimators=100, random_state=1)
    fit = rfr.fit(train_x, train_y)

    # noinspection PyTypeChecker
    find_accuracy(fit, dataframe, new_dataframe, factor, forecast_length)

    forecast_values = []
    input_data = new_dataframe.loc[end_point:, ~new_dataframe.columns.isin(['Date', factor])]
    prediction = fit.predict(input_data)

    for i in range(end_point):
        forecast_values.append(np.NAN)
    for i in range(forecast_length):
        forecast_values.append(prediction[i])

    new_dataframe['forecast_'+factor] = forecast_values
    new_dataframe = new_dataframe.drop(columns=['Day', 'Month'])

    return new_dataframe


def save_excel(excel_data, sheet_name, loc, folder):
    excel_data = excel_data.fillna(' ')
    excel_data['Date'] = excel_data['Date'].dt.date

    excel_data.to_excel(excel_writer=folder + loc + '_' + sheet_name + '.xlsx', sheet_name=loc + '_' + sheet_name, index=False)


def save_plots(excel_data, factor, forecast_length, loc, folder):
    excel_data = excel_data.fillna(0.0)
    matplotlib.use('Agg')
    plt.figure(figsize=(14, 4))

    if get_factor(factor):
        plt.plot(excel_data['Date'][:-forecast_length], excel_data[factor][:-forecast_length], color='blue')
        plt.plot(excel_data['Date'][-forecast_length:], excel_data['forecast_' + factor][-forecast_length:], color='red')
    else:
        plt.bar(excel_data['Date'], excel_data[factor], color='blue')
        plt.bar(excel_data['Date'], excel_data['forecast_'+factor], color='red')

    plt.xlabel('Date')
    plt.ylabel(factor)
    plt.legend(['Actual', 'Forecast'])
    plt.suptitle(loc + '_' + factor)
    plt.savefig(folder+'{}_{}'.format(loc, factor) + '.png', bbox_inches='tight', pad_inches=0)
    plt.show()

    plt.close("all")
