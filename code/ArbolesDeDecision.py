import findspark
from pyspark.sql import SparkSession
from time import time
from pyspark.mllib.tree import DecisionTree
from pyspark.mllib.regression import LabeledPoint
from numpy import array
from utils import load_data
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, RegressionEvaluator
from pyspark.sql.functions import col

# init spark
findspark.init()
spark = SparkSession.builder \
    .appName("ArbolesDeDecision") \
    .getOrCreate()

# Load the data
url = "http://kdd.ics.uci.edu/databases/kddcup99/kddcup.data.gz"
filename = "kddcup.data.gz"
df = load_data(url, filename, spark)
print ("El tamaño de los datos de entrenamiento es {}".format(df.count()))

test_data_file = "corrected.gz"
url_test = "http://kdd.ics.uci.edu/databases/kddcup99/corrected.gz"
df_test = load_data(url_test, test_data_file, spark)
print ("El tamaño de los datos de test es {}".format(df_test.count()))
print("\n")

# Decision Tree
data = df.rdd.map(lambda d: d.value.split(","))
data_test = df_test.rdd.map(lambda d: d.value.split(","))

data = data.repartition(100) 
data_test = data.repartition(50)
 
protocols = data.map(lambda d: d[1]).distinct().collect()
services = data.map(lambda d: d[2]).distinct().collect()
flags = data.map(lambda d: d[3]).distinct().collect()

def process_line(line_split, protocols, services, flags):
    new_line = line_split[0:41]  # exclude = [41]
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
        
    return LabeledPoint(attack, array([float(x) for x in new_line]))

training_data = data.map(lambda x: process_line(x, protocols, services, flags))

test_data = data_test.map(lambda x: process_line(x, protocols, services, flags))

t0 = time()
tree_model = DecisionTree.trainClassifier(training_data, numClasses=2, categoricalFeaturesInfo={1: len(protocols), 2: len(services),3: len(flags)},
                                          impurity='gini', maxDepth=4, maxBins=100)
tt = time() - t0

print ("Se ha entrenado en {} segundos".format(round(tt,3)))
print("\n")

# Evaluating the model on test data
predictions = tree_model.predict(test_data.map(lambda p: p.features))
labels_preds = test_data.map(lambda t: t.label).zip(predictions)

# Displaying the tree model
print ("Modelo de árbol de clasificación:")
print (tree_model.toDebugString())
print("\n")
print("\n")


# Evaluating the model
print("Estadísticas del modelo: ")

evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction")

df_labels_preds = labels_preds.toDF(["label", "prediction"])
df_labels_preds = df_labels_preds.withColumn("prediction", col("prediction").cast("double"))

accuracy = evaluator.evaluate(df_labels_preds, {evaluator.metricName: "accuracy"})
precision = evaluator.evaluate(df_labels_preds, {evaluator.metricName: "weightedPrecision"})
recall = evaluator.evaluate(df_labels_preds, {evaluator.metricName: "weightedRecall"})
f1 = evaluator.evaluate(df_labels_preds, {evaluator.metricName: "f1"})

print("Exactitud: {}".format(accuracy))
print("Precision: {}".format(precision))
print("Sensibilidad: {}".format(recall))
print("F1 Score: {}".format(f1))

spark.stop()
