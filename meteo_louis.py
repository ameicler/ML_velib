
import numpy as np
import pandas as pd
import datetime
from datetime import datetime
import time
from datetime import datetime
import json


# Meteo processing
def get_meteo():
  # Load data
  data = _load_data()
  # Clean column utcdate
  data['utcdate'] = data['utcdate'].map(_clean_date)
  # Clean wind
  data['wspdm'] = data['wspdm'].map(lambda x: float(x))
  # select columns
  data = _select_columns(data)
  # pass utc_date as index
  data = _utcdate_index(data)
  # rename utcdate into date
  _rename_date(data)
  # dummify the meteo conditions
  data = _dummify(data)
  return data

def _load_data():
  path = '/Users/louis/Documents/Snips/snips_test/challenge_data/stations_data/paris_weather_20150831_20151130.jsonr'
  # open and read json
  raw_file = open(path).read()
  # load json
  raw_json = json.loads(raw_file)
  # select only data
  meteo_data = [el['data'] for el in raw_json]
  #create dataframe
  data = pd.DataFrame(meteo_data)
  return data

# Method that takes a hash as input and returns a '%Y-%m-%d %H:%M:%S' formatted date
def _clean_date(date_hash):
    ymd = '-'.join((date_hash['year'], date_hash['mon'], date_hash['mday']))
    hms = ':'.join((date_hash['hour'], date_hash['min'], '00'))
    date = ymd + ' ' + hms
    return datetime.strptime(date, '%Y-%m-%d %H:%M:%S')


def _select_columns(data):
  selected_columns = ['wspdm', 'utcdate', 'rain', 'conds']
  data = data.loc[:, selected_columns]
  return data

def _utcdate_index(data):
  data.set_index('utcdate', inplace = True)
  return data

def _rename_date(data):
  data.reset_index(drop = False, inplace = True)
  data.rename(columns={'utcdate': 'date'}, inplace=True)
  return data

def _dummify(data):
  # --> separate out the conditions
  data_cond = data[['date','conds']]
  # --> pass date as index
  data_cond.set_index('date', inplace = True)
  # --> dummify conds
  data_cond_dumm = pd.get_dummies(data_cond, prefix = 'cond', dummy_na = 'unknown', prefix_sep = "_")
  # --> drop empty column nan
  del data_cond_dumm['cond_nan']
  # --> remerge with dataset
  data = data.merge(data_cond_dumm, left_on = 'date', right_index = True, how = 'inner')
  # --> drop the conditions columns
  del data['conds']
  # --> reset the date as index
  data.set_index('date', drop=True, inplace=True)
  return data








