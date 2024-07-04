import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import split
from graphframes import GraphFrame
from pyspark.sql.functions import col
from pyspark.sql.functions import expr
from time import time
from utils import load_data

# init spark
findspark.init()
spark = SparkSession.builder \
    .appName("IntroduccionSpark") \
    .getOrCreate()


# Load the data
url = "http://kdd.ics.uci.edu/databases/kddcup99/kddcup.data.gz"
filename = "kddcup.data.gz"

df = load_data(url, filename, spark)

print("Numero total de elementos: ",df.count())
print("Mostramos 5 elementos: ",df.take(5))
print("\n")


#GraphFrames
# Split the "value" column into different columns
df_split = df.withColumn("values", split(df.value, ",")).selectExpr("values[0] as id", "values[1] as protocol", "values[2] as service", "values[3] as flag", "values[41] as attack_type")
nodes = df_split.select("id").distinct() # Create a DataFrame of nodes (vertices) with the column "id" as the identifier
edges = df_split.selectExpr("protocol as src", "service as dst", "attack_type as relationship") # Create a DataFrame of edges
graph = GraphFrame(nodes, edges)

print("Número de nodos:", graph.vertices.count())
print("Número de aristas:", graph.edges.count())
print("\n")

connections_with_attack = graph.edges.filter("relationship = 'buffer_overflow.'") # Finding the connections associated with a specific type of attack
print("Graphframes conexiones con ataques buffer_overflow: ")
connections_with_attack.show()

ranks = graph.pageRank(resetProbability=0.15, maxIter=10)
top_ranks = ranks.vertices.orderBy(ranks.vertices.pagerank.desc()).limit(10)
print("Graphframes TopRanks: ")
top_ranks.show()
print("\n")


# Calculating statistics
split_column = split(df['value'], ',')
df_column = df.withColumn('features', split_column)

value_column = df_column.select(col("features")[0].cast("float").alias("duration"), # number of seconds of the connection
                                     col("features")[4].cast("float").alias("src_bytes"), # number of data bytes from source to destination 
                                     col("features")[8].cast("float").alias("urgent")) # number of urgent packets 

print("Calculos de estadisticas para duration, src_bytes y urgent: ")
value_column.describe().show()
print("\n")



# Filtering by attack type
t0 = time()

neptune_df = df.filter(expr("value LIKE '%neptune.%'"))
neptune_count = neptune_df.count()

t1 = time() - t0

print("Filtración por el ataque neptune: ")
neptune_df.show(5, truncate=False)
print ("Hay {} ataques 'neptune'".format(neptune_count))
print ("Tiempo de ejecución {} segundos".format(round(t1,3)))
print("\n")


# map() function
def parse_line_py(line):
    elems = line.value.split(",")
    return (elems[41], elems) # elems[41] is the position of the tag

t0 = time()
dict_data = df.rdd.map(parse_line_py)
head_rows = dict_data.take(4)
t1 = time() - t0

print("Mapeo de los datos: ")
print(head_rows[0])
print ("Tiempo de ejecución {} segundos".format(round(t1,3)))

spark.stop()