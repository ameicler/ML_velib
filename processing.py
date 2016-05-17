#!/usr/bin/env python
# -*- coding: utf8 -*-

import numpy as np
from pprint import pprint
import operator

import pandas as pd #this is how I usually import pandas

import gzip
import json
from os import listdir


def zipped_file(f):
    return True if ".gz" in f else False

def merge_station_data(path):
    """ merging all stations data into a single row-indexed dataframe """
    data = None
    stations = [f for f in listdir(path) if zipped_file(f)]
    for station in stations:
        print station
        df = jsonr_to_df( path + station )
        if df is not None:
            if data is not  None:
                print df.shape
                data = pd.concat([data,df])
                print data.shape
            else:
                data = df
                print data.shape
    data = data.reset_index()
    return data


def jsonr_to_df(filename):
    """ converting json file to a cleaned dataframe """
    with gzip.open(filename, 'rb') as f:
        # ==> Loading and parsing compressed files
        station_data = json.loads(f.read())
        if len(station_data) > 0:
            # ==> Builind data frame
            df = pd.DataFrame(station_data)
            # ==> Cleaning position
            df["lat"]=df["position"].map(lambda x: x['lat'])
            df["lng"]=df["position"].map(lambda x: x['lng'])
            del df["position"]
            return df
        else:
            return None

data = merge_station_data('challenge_data/stations_data/')

data.to_json(path_or_buf = 'challenge_data/stations_merged.json')
