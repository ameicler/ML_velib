import numpy as np
import pandas as pd
import datetime
from datetime import datetime
import time
from datetime import datetime
import json

# master method
def data_model(data,data_meteo , sampling_time, output):
  #Get parameters that vary over time
  data_left = _data_left_model(data)
  # Resample
  data_left = _resample_data(data_left, sampling_time)
  # get static info
  data_static = _static_info(data)
  # Merge static and timeseries
  data_model = _merge_static_resampled(data_left, data_static)
  # dummify arrondissements
  data_model = _dummify_arrondissements(data_model)
  # Drop arrondissements column
  data_model.reset_index(['arrondissements'], drop = True, inplace = True)
  # Create a boolean for week ends
  data_model = _weekend(data_model)
  # Add dummies for hours
  data_model = _dummify_hours(data_model)
  # select and order features
  data_model.reset_index('date', drop = False, inplace = True)
  # merge data and data meteo
  data_model = data_model.merge(data_meteo, left_on = 'date', right_index = True)
  # reset address and date as index
  data_model.set_index(['address','date'], drop = True, inplace = True)
  # select columncs
  #data_model = _select_and_order(data_model, output)
  return data_model

def _data_left_model(data):
  # select useful columns
  useful_columns = ['date','address', 'av_ratio', 'available_bike_stands', 'available_bikes', 'bike_stands', 'tot_bikes']
  data = data[useful_columns]
  # set date as index to allow resampling
  data.set_index(['date'], drop = True, inplace = True)
  return data

# Generic method to resample data
def _resample_data(data, sampling_time):
  # groupby and resample
  data_resampled = data.groupby('address').resample(str(sampling_time) + 'min').mean().ffill()
  return data_resampled

def _static_info(data):
  # pivot to get the desired table
  data_static = data.pivot_table(index = ['address', 'name', 'number', 'lat', 'lng','arrondissements'], aggfunc = np.max)
  # reset index, keeping solely address as index
  selected_columns = ['name', 'number', 'lat', 'lng','arrondissements']
  data_static.reset_index(selected_columns, drop = False, inplace = True)
  # select columns
  data_static = data_static[selected_columns]
  return data_static

# Merge resampled data and static data
def _merge_static_resampled(data_resampled, data_static):
  data_resampled.reset_index(['date', 'address'], drop = False, inplace = True)
  data_model = data_resampled.merge(data_static, left_on = 'address', right_index = True, how = 'left' )
  return data_model

# dummify arrondissements
def _dummify_arrondissements(data_model):
  # Create pivot table with address and arrondissements
  add_arr = pd.pivot_table(data_model, index = ['address', 'arrondissements']).reset_index(['address', 'arrondissements'])
  # select address and arrondissements
  add_arr = add_arr[['address', 'arrondissements']]
  # Pass address as index
  add_arr.set_index('address', drop = True, inplace = True)
  # Transform arrondissements into strings
  add_arr['arrondissements'] = add_arr['arrondissements'].map(lambda x: str(x))
  # Dummify ( convert int to string to allow dummify)
  add_arr_dumm = pd.get_dummies(add_arr, prefix = 'arr', prefix_sep = "_")
  # merge data_model and dummified arrondissements
  data_output = data_model.merge(add_arr_dumm, left_on = 'address', right_index = True, how = 'left')
  # set address and date as index
  data_output.set_index(['address', 'arrondissements'], drop = True, inplace = True)
  return data_output

# create week-ends (true) - weekdays(false)
def _weekend(data):
  # Create a column with the day
  data['week_end'] = data['date'].map(lambda x: x.day)
  # create a dic
  dic = {0:False, 1: False, 2: False, 3:False, 4:False, 5:True, 6:True}
  # map days to the dic (week end or not)
  data['week_end'] = data['week_end'].map(dic)
  return data

def _dummify_hours(data):
  # create a column of hours
  data['hour'] = data['date'].map(lambda x: x.hour)
  # turn hours into strings
  data['hour'] = data['hour'].map(lambda x: str(x))
  # create a separate dataframe to create dummies
  data_hour = data[['date', 'hour']]
  # pass date as index
  data_hour.set_index('date', drop = True, inplace = True)
  # dummify
  data_hour_dumm = pd.get_dummies(data_hour, prefix = 'hour', prefix_sep = '_')
  # delete column hours
  del data['hour']
  # merge
  data = data.merge(data_hour_dumm, left_on = 'date', right_index = True, how = 'left')
  return data

def _select_and_order(data, output):
  #hours
  hours = ['hour_%i' %i for i in range(0, 24)]
  # arr
  arr = ['arr_1', 'arr_4', 'arr_7']
  # selected column
  selected_columns = [output] + hours + arr
  # select on data
  data = data[selected_columns]
  return data

