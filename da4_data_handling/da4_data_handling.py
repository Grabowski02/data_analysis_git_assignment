# the Google colab session kept crashing so I implemented locally

import re
import time
import pandas as pd
import dask.dataframe as dd
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
import tracemalloc


def process_text(text):
    words = re.findall(r'\w+', text)  # Tokenize words
    word_count = len(words)  # Count words
    sentiment_score = sum(ord(c) for c in text) % 100  # Fake sentiment calculation
    return word_count, sentiment_score


df_pd = pd.read_csv("Books_rating.csv")['review/text'].astype(str)  # The scv file has to be next to this code

print("Reading over")

start = time.time()
df_pd.apply(process_text)
stop = time.time()
t_pd = stop - start

print("Pandas over")

dask_dt = dd.from_pandas(df_pd).fillna('').compute()

start = time.time()
dask_dt.apply(process_text)
stop = time.time()
t_dusk = stop - start

print("Dusk over")

plt.bar(["pandas run time", "dusk run time"], [t_pd, t_dusk])
plt.ylabel("time [s]")
plt.title("Comparing pandas and dusk run times")
plt.show()

tracemalloc.start()
df_pd.head(10000).apply(process_text)  # I only checked memory usage for the first ten thousand rows because otherwise
# it took too much time
mem_pd = tracemalloc.get_traced_memory()
tracemalloc.stop()

tracemalloc.start()
df_pd.head(10000).apply(process_text)
mem_dusk = tracemalloc.get_traced_memory()
tracemalloc.stop()

plt.bar(["pandas memory usage", "dusk memory usage"], [mem_pd[1], mem_dusk[1]])
plt.title("Comparing pandas and dusk memory usage")
plt.ylabel("Memory")
plt.show()

# I couldn't get pyspark to work

# spark = SparkSession.builder.master("local").getOrCreate()
#
# spark_df = spark.createDataFrame(df_pd[['review/text']])
#
# spark_df.show()
