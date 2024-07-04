import findspark
from pyspark.sql import SparkSession
from pyspark.ml.classification import LinearSVC
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, RegressionEvaluator
from pyspark.sql import Row
from pyspark.ml.linalg import Vectors
from utils import load_data
from pyspark.sql.functions import col
from time import time

# init spark
findspark.init()
spark = SparkSession.builder \
    .appName("SVM") \
    .getOrCreate()

# Load the data
url = "http://kdd.ics.uci.edu/databases/kddcup99/kddcup.data.gz"
filename = "kddcup.data.gz"
df = load_data(url, filename, spark)
print("El tamaño de los datos de entrenamiento es {}".format(df.count()))

test_data_file = "corrected.gz"
url_test = "http://kdd.ics.uci.edu/databases/kddcup99/corrected.gz"
df_test = load_data(url_test, test_data_file, spark)
print("El tamaño de los datos de test es {}".format(df_test.count()))
print("\n")

data = df.rdd.map(lambda d: d.value.split(","))
data_test = df_test.rdd.map(lambda d: d.value.split(","))

data = data.repartition(100) 
data_test = data.repartition(50)

# Get distinct values for each column
protocols = data.map(lambda d: d[1]).distinct().collect()
services = data.map(lambda d: d[2]).distinct().collect()
flags = data.map(lambda d: d[3]).distinct().collect()

def process_line(line_split, protocols, services, flags):
    new_line = line_split[0:41]
    try: 
        new_line[1] = protocols.index(new_line[1])
    except:
        new_line[1] = len(protocols)

    try:
        new_line[2] = services.index(new_line[2])
    except:
        new_line[2] = len(services)
    
    try:
        new_line[3] = flags.index(new_line[3])
    except:
        new_line[3] = len(flags)
    
    if line_split[41] == 'normal.':
        attack = 0.0
    else:
        attack = 1.0
        
    return Row(label=attack, features=Vectors.dense([float(x) for x in new_line]))

# Prepare the data
training_data = data.map(lambda x: process_line(x, protocols, services, flags))
test_data = data_test.map(lambda x: process_line(x, protocols, services, flags))

training_df = spark.createDataFrame(training_data)
test_df = spark.createDataFrame(test_data)

# Train the model
t0 = time()
svm = LinearSVC(labelCol="label", featuresCol="features", maxIter=10)

model = svm.fit(training_df)
print("Modelo de Support Vector Machine (SVM) entrenado")
print("Tiempo de entrenamiento: {} segundos".format(round(time() - t0,3)))
print("\n")

# Make predictions
predictions = model.transform(test_df)

print("Estadísticas del modelo: ")

evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction")

df_labels_preds = predictions.select("label", "prediction").withColumn("prediction", col("prediction").cast("double"))

accuracy = evaluator.evaluate(df_labels_preds, {evaluator.metricName: "accuracy"})
precision = evaluator.evaluate(df_labels_preds, {evaluator.metricName: "weightedPrecision"})
recall = evaluator.evaluate(df_labels_preds, {evaluator.metricName: "weightedRecall"})
f1 = evaluator.evaluate(df_labels_preds, {evaluator.metricName: "f1"})

print("Exactitud: {}".format(accuracy))
print("Precision: {}".format(precision))
print("Sensibilidad: {}".format(recall))
print("F1 Score: {}".format(f1))

spark.stop()
