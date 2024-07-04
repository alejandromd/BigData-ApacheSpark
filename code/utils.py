from pyspark.mllib.stat import Statistics 
from pyspark.mllib.regression import LabeledPoint
from numpy import array
import pandas as pd
import numpy as np
from math import sqrt 
import os
import urllib.request

def load_data(url, filename, spark):
    if not os.path.isfile(filename):
        urllib.request.urlretrieve(url, filename)

    df = spark.read.text(filename)
    return df
    
def parse_line_correlation(line):
    line_split = line.value.split(",")
    new_line = line_split[0:1]+line_split[4:25]+line_split[26:27]+line_split[28:35]+line_split[36:38]+line_split[39:40] # exclude = [1,2,3,25,27,35,38,40,41]
    attack = 1.0
    if line_split[41]=='normal.':
        attack = 0.0
    return LabeledPoint(attack, array([float(x) for x in new_line]))

def parse_line_categorical(line):
    line_split = line.value.split(",")
    new_line = line_split[6:41]
    attack = 1.0
    if line_split[41]=='normal.':
        attack = 0.0
    return LabeledPoint(attack, array([float(x) for x in new_line]))

def parse_interaction_chi(line):
    line_split = line.value.split(",")
    new_line = line_split[0:1] + line_split[4:6] + line_split[7:19] + line_split[20:41] # exclude = [1,2,3,6,19,41]
    attack = 1.0
    if line_split[41]=='normal.':
        attack = 0.0
    return LabeledPoint(attack, array([float(x) for x in new_line]))

def parse_line_array(line):
    line_split = line.value.split(",")
    exclude_columns = [1,2,3,41]
    new_line = [item for i,item in enumerate(line_split) if i not in exclude_columns]
    return np.array([float(x) for x in new_line])

def parse_line_by_tag(line):
    line_split = line.value.split(",")
    exclude_columns = [1,2,3,41]
    new_line = [item for i,item in enumerate(line_split) if i not in exclude_columns]
    return (line_split[41], np.array([float(x) for x in new_line]))

def summary_by_label(dataframe, label):
    tag_vector_data = dataframe.rdd.map(parse_line_by_tag).filter(lambda d: d[0]==label)
    return Statistics.colStats(tag_vector_data.values())

def get_variable_stats(stats_by_label, num_column):
    column_stats_by_label = [
        (stat[0], np.array([float(stat[1].mean()[num_column]), float(sqrt(stat[1].variance()[num_column])), float(stat[1].min()[num_column]), float(stat[1].max()[num_column]), int(stat[1].count())])) 
        for stat in stats_by_label
    ]

    data_dict = {
        "Etiqueta": [label[0] for label in column_stats_by_label],
        "Media": [values[0] for label, values in column_stats_by_label],
        "Desviacion estandar": [values[1] for label, values in column_stats_by_label],
        "Minimo": [values[2] for label, values in column_stats_by_label],
        "Maximo": [values[3] for label, values in column_stats_by_label],
        "Numero total": [int(values[4]) for label, values in column_stats_by_label],
    }
    return pd.DataFrame(data_dict, columns=["Etiqueta", "Media", "Desviacion estandar", "Minimo", "Maximo", "Numero total"])

