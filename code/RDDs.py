import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from time import time
from utils import load_data

# init spark
findspark.init()
spark = SparkSession.builder \
    .appName("RDDs") \
    .getOrCreate()


# Load the data
url = "http://kdd.ics.uci.edu/databases/kddcup99/kddcup.data.gz"
filename = "kddcup.data.gz"

df = load_data(url, filename, spark)
df_sample = df.sample(False, 0.1)

sample_size = df_sample.count()
total_size = df.count()

print ("El tamaño de la muestra es {} de {}".format(sample_size, total_size))
print("\n")

# Normal interactions
df_sample_items = df_sample.rdd.map(lambda d: d["value"].split(","))
df_sample_normal = df_sample_items.filter(lambda d: "normal." in d)

t0 = time()
df_sample_normal_count = df_sample_normal.count()
t1 = time() - t0

sample_normal_ratio = df_sample_normal_count / float(sample_size)
print ("La relación de interacciones 'normales' es {}".format(round(sample_normal_ratio,3))) 
print ("Tiempo de ejecución {} segundos".format(round(t1,3)))
print("\n")


# Attack interactions
df_normal = df.filter(col("value").contains("normal."))
df_count = df.count()
df_normal_count = df_normal.count()
df_attack = df.exceptAll(df_normal)
df_attack_count = df_attack.count()

print ("Hay {} interacciones normales y {} ataques, de un total de {} interacciones".format(df_normal_count,df_attack_count,df_count))
print("\n")


# cartesian product
data = df.rdd.map(lambda d: d.value.split(","))
protocols = data.map(lambda d: d[1]).distinct()
protocols.collect()

services = data.map(lambda d: d[2]).distinct()
services.collect()
product = protocols.cartesian(services).collect()
print ("Existen {} combinaciones de protocolo-servicio".format(len(product)))
print("\n")


# Duration of the connection
normal_data = data.filter(lambda d: d[41]=="normal.")
attack_data = data.filter(lambda d: d[41]!="normal.")
normal_duration_data = normal_data.map(lambda d: int(d[0]))
attack_duration_data = attack_data.map(lambda d: int(d[0]))

dict_data = data.map(lambda d: (d[41], d)) # d[41] contains the network interaction tag
dict_duration = data.map(lambda d: (d[41], float(d[0]))) 
sum_counts = dict_duration.combineByKey(
    (lambda x: (x, 1)), # the initial value, with value x and count 1
    (lambda acc, value: (acc[0]+value, acc[1]+1)), # how to combine a pair value with the accumulator: sum value, and increment count
    (lambda acc1, acc2: (acc1[0]+acc2[0], acc1[1]+acc2[1])) # combine accumulators
)

print("Pares clave-valor de la duración de la conexión, en formato 'tag:(suma total, recuento)': ")
print(sum_counts.collectAsMap())
print("\n")


# Calculate the mean duration of the connection for each tag
duration_means = sum_counts.map(lambda d: (d[0], round(d[1][0] / d[1][1], 3))).collectAsMap()

print("Duración media de la conexión para cada etiqueta: ")
for tag in sorted(duration_means, key=duration_means.get, reverse=True):
    print (tag, duration_means[tag])

spark.stop()