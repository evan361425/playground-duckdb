import pandas as pd
import duckdb as duck

import plotly.plotly as py # interactive graphing
import plotly.graph_objs as go

conn = duck.connect('data/xray.db')
df = pd.read_sql('SELECT url_clean_path, AVG(duration) as `avg_duration`'
                       'FROM data '
                       'GROUP BY url_clean_path '
                       'ORDER BY -avg_duration', conn)

py.iplot([go.Bar(x=df.url_clean_path, y=df.avg_duration)], filename='test')
