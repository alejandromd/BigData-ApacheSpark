import findspark
from pyspark.sql import SparkSession
from pyspark.mllib.stat import Statistics 
from pyspark.mllib.classification import LogisticRegressionWithLBFGS
from time import time
import pandas as pd
from pyspark.sql.functions import col
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, RegressionEvaluator
from utils import parse_line_categorical, parse_interaction_chi, load_data

pd.set_option('display.max_colwidth', 30)


# init spark
findspark.init()
spark = SparkSession.builder \
    .appName("RegresionLogistica") \
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


# Logistic Regression
feature_names = ["land","wrong_fragment",
             "urgent","hot","num_failed_logins","logged_in","num_compromised",
             "root_shell","su_attempted","num_root","num_file_creations",
             "num_shells","num_access_files","num_outbound_cmds",
             "is_hot_login","is_guest_login","count","srv_count","serror_rate",
             "srv_serror_rate","rerror_rate","srv_rerror_rate","same_srv_rate",
             "diff_srv_rate","srv_diff_host_rate","dst_host_count","dst_host_srv_count",
             "dst_host_same_srv_rate","dst_host_diff_srv_rate","dst_host_same_src_port_rate",
             "dst_host_srv_diff_host_rate","dst_host_serror_rate","dst_host_srv_serror_rate",
             "dst_host_rerror_rate","dst_host_srv_rerror_rate"]



training_data_categorical = df.rdd.map(parse_line_categorical)
chi = Statistics.chiSqTest(training_data_categorical)

records = [(result.statistic, result.pValue) for result in chi]

df_chi = pd.DataFrame(data=records, index= feature_names, columns=["Statistic","pvalue"])

print("Chi test: ")
print(df_chi)

training_data = df.rdd.map(parse_interaction_chi)
test_data = df_test.rdd.map(parse_interaction_chi)

t0 = time()
logit_model_chi = LogisticRegressionWithLBFGS.train(training_data)
tt = time() - t0

print ("Se ha entrenado en {} segundos".format(round(tt,3)))
print("\n")

labels_preds = test_data.map(lambda t: (t.label, logit_model_chi.predict(t.features)))


# Evaluating the model
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