import pandas as pd
import duckdb as duck
import seaborn as sns
import numpy as np
import matplotlib.pyplot as plt

conn = duck.connect("data/poc.db")
df = pd.read_sql(
    "SELECT duration FROM xray " "WHERE url_clean_path = '/some-path' ", conn
)

plot = sns.displot(data=df, x="duration", kde=True)
plt.savefig("output.png")
