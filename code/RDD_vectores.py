import findspark
from pyspark.sql import SparkSession
from pyspark.mllib.stat import Statistics 
from math import sqrt 
import pandas as pd
from utils import *

pd.set_option('display.max_columns', 50)


# init spark
findspark.init()
spark = SparkSession.builder \
    .appName("RDD_vectores") \
    .getOrCreate()

# Load the data
url = "http://kdd.ics.uci.edu/databases/kddcup99/kddcup.data_10_percent.gz"
filename = "kddcup.data_10_percent.gz"
df = load_data(url, filename, spark)

vector_data = df.rdd.map(parse_line_array)


# Calculating statistics
summary = Statistics.colStats(vector_data)

print ("Estadísticas de duración:") 
print (" Media: {}".format(round(summary.mean()[0],3))) # 0 is the position of the duration, rounded to 3 decimals
print (" Desviación estándar: {}".format(round(sqrt(summary.variance()[0]),3)))
print (" Valor máximo: {}".format(round(summary.max()[0],3)))
print (" Valor mínimo: {}".format(round(summary.min()[0],3)))
print (" Conteo total: {}".format(summary.count()))
print (" Número de 'no ceros': {}".format(summary.numNonzeros()[0]))
print("\n")


normal_summary = summary_by_label(df, "normal.")

print ("Estadísticas de duración para: {}".format("normal"))
print (" Media: {}".format(normal_summary.mean()[0],3)) # 0 is the position of the duration, rounded to 3 decimals
print (" Desviación estándar: {}".format(round(sqrt(normal_summary.variance()[0]),3)))
print (" Valor máximo: {}".format(round(normal_summary.max()[0],3)))
print (" Valor mínimo: {}".format(round(normal_summary.min()[0],3)))
print (" Conteo total: {}".format(normal_summary.count()))
print (" Número de 'no ceros': {}".format(normal_summary.numNonzeros()[0]))
print("\n")


guess_passwd_summary = summary_by_label(df, "guess_passwd.") 

print ("Estadísticas de duración para: {}".format("guess_password"))
print (" Media: {}".format(guess_passwd_summary.mean()[0],3)) # 0 is the position of the duration, rounded to 3 decimals
print (" Desviación estándar: {}".format(round(sqrt(guess_passwd_summary.variance()[0]),3)))
print (" Valor máximo: {}".format(round(guess_passwd_summary.max()[0],3)))
print (" Valor mínimo: {}".format(round(guess_passwd_summary.min()[0],3)))
print (" Conteo total: {}".format(guess_passwd_summary.count()))
print (" Número de 'no ceros': {}".format(guess_passwd_summary.numNonzeros()[0]))
print("\n")

# Calculating statistics for all labels
label_list = ["back.","buffer_overflow.","ftp_write.","guess_passwd.","imap.","ipsweep.","land.","loadmodule.","multihop.",
              "neptune.","nmap.","normal.","perl.","phf.","pod.","portsweep.","rootkit.","satan.","smurf.","spy.","teardrop.",
              "warezclient.","warezmaster."]

stats_by_label = [(label, summary_by_label(df, label)) for label in label_list]

print ("Estadísticas de la duración por etiquetas")
print(get_variable_stats(stats_by_label,0)) # 0 is the position of the duration
print("\n")

print ("Estadísticas de 'src_bytes' por etiquetas")
print(get_variable_stats(stats_by_label,1)) # 1 is the position of src_bytes
print("\n")

correlation_matrix = Statistics.corr(vector_data, method="spearman")

column_names = ["duration","src_bytes","dst_bytes","land","wrong_fragment",
             "urgent","hot","num_failed_logins","logged_in","num_compromised",
             "root_shell","su_attempted","num_root","num_file_creations",
             "num_shells","num_access_files","num_outbound_cmds",
             "is_hot_login","is_guest_login","count","srv_count","serror_rate",
             "srv_serror_rate","rerror_rate","srv_rerror_rate","same_srv_rate",
             "diff_srv_rate","srv_diff_host_rate","dst_host_count","dst_host_srv_count",
             "dst_host_same_srv_rate","dst_host_diff_srv_rate","dst_host_same_src_port_rate",
             "dst_host_srv_diff_host_rate","dst_host_serror_rate","dst_host_srv_serror_rate",
             "dst_host_rerror_rate","dst_host_srv_rerror_rate"]

df_correlation = pd.DataFrame(correlation_matrix, index=column_names, columns=column_names)

print ("Matriz de correlación de Spearman")
print(df_correlation)
print("\n")

# High correlation
boolean_high_correlation = (abs(df_correlation) > .8) & (df_correlation < 1.0)

correlation_index = (boolean_high_correlation==True).any()
correlation_names = correlation_index[correlation_index==True].index

print("Matriz de alta correlación")
print(boolean_high_correlation.loc[correlation_names,correlation_names])

spark.stop()