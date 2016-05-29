from __future__ import division #import dependency so division does not raise an int
# Importing dependencies
import numpy as np
import pandas as pd
import datetime
#from datetime import datetime
import calendar #necessary to convert a timestamp into a date
#from datetime import datetime
import time

def processing_data():
  # Read Json and create dataframe
  data = _json_to_df()
  # Convert time stamps
  data['date'] = data['last_update'].map(_to_datetime)
  # Process timestamps and create hour, day, and week columns
  _timestamps_processing(data)
  # create arrondissements
  data['arrondissements'] = data.apply(_arrondissements, axis = 1)
  data['arrondissements'] = data['arrondissements'].map(lambda x: int(float(x)))
  # add tot bikes
  data['tot_bikes'] = data.apply(_tot_bikes, axis = 1)
  # Apply compute_delta_capacity and return a serie
  delta = data.apply(_compute_delta_capacity, axis = 1)
  # Create availibility ratio
  data['av_ratio'] = data.apply(_availibility_ratio, axis = 1)
  return data


# Read Json and create dataframe
def _json_to_df():
  # Built-in pandas method to read json
  path_json = '/Users/louis/Documents/Snips/snips_test/challenge_data/stations_merged.json'
  data = pd.read_json(path_json)
  return data

# Method that converts the timestamp into date format
def _to_datetime(timestamp):
  date = datetime.datetime.fromtimestamp(timestamp/1000).strftime('%Y-%m-%d %H:%M:%S')
  date_output = datetime.datetime.strptime(date, '%Y-%m-%d %H:%M:%S')
  return date_output

def _timestamps_processing(data):
  # Create hours, days, and week
  data['hour'] = data['date'].map(lambda x: x.hour)
  data['day'] = data['date'].map(lambda x: x.day)
  data['week'] = data['date'].map(lambda x: x.week)
  return data

  #Create arrondissements
def _arrondissements(row):
  first_case = row['address'].split('-')[1].split(' ')[1][-2:]
  second_case = row['address'].split('-')[-1].split(' ')[1][-2:]

  if first_case != 'RE':
      return first_case
  else:
      return second_case

# add a columns tot_bikes
def _tot_bikes(row):
  return row['available_bikes'] + row['available_bike_stands']

# Is the number of bike available + number of slots available = capacity
def _compute_delta_capacity(row):
  return row['available_bike_stands'] +row['available_bikes'] - row['bike_stands']

# create a new column with the availibility ratio
def _availibility_ratio(row):
  return row['available_bikes']/(row['available_bikes'] + row['available_bike_stands'])

# Process timestamps

# # RAPPEL: date types
# # a is a datetime object
# a  = datetime.datetime(2015,11,13,0,0,0)
# # b is a string
# b = start_date.strftime('%Y-%m-%d %H:%M:%S')
# # c is a datetime object
# c = datetime.datetime.strptime(b, '%Y-%m-%d %H:%M:%S')
# # RAPPEL: timestamps
# # d is a datetime object
# d = datetime.datetime.fromtimestamp(1342973940)





