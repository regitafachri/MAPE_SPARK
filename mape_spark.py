from pyspark.sql.functions import *
from pyspark.sql import functions as F

def mean_absolute_percentage_error(data, label, prediction, n):
    data = data.withColumn('min', expr("abs(label - prediction)/label"))
    data = data.withColumn('min',abs(data.min))
    summin = data.select(F.sum('min')).collect()[0][0]
    mean = summin / n
    mape = mean * 100
    return mape