import pandas as pd
import duckdb as duck
import seaborn as sns

conn = duck.connect("data/prod.db")
df = pd.read_sql(
    "SELECT * FROM ("
    "SELECT url_clean_path, avg(duration) AS avg_duration, stddev(duration) AS stddev_duration,  count(*) AS data_count "
    "FROM xray GROUP BY url_clean_path) AS t "
    "WHERE data_count > 3000 ORDER BY avg_duration DESC LIMIT 5",
    conn,
)

highlights = df.to_dict(orient="list")
highlights_details = pd.read_sql(
    "SELECT * FROM xray WHERE url_clean_path IN ('%s')"
    % "','".join(highlights["url_clean_path"]),
    conn,
)

g = sns.FacetGrid(
    highlights_details, row="url_clean_path", row_order=highlights["url_clean_path"]
)
g.map(sns.histplot, "duration", binwidth=0.5, binrange=(0, 10))

g.set_titles(row_template="{row_name}")

for path, avg_duration, stddev_duration in zip(
    highlights["url_clean_path"],
    highlights["avg_duration"],
    highlights["stddev_duration"],
):
    ax = g.axes_dict[path]
    ax.axvline(avg_duration, color="k", linestyle="dashed", linewidth=1)
    _, ylim_max = ax.get_ylim()
    ax.text(
        7,
        ylim_max * 0.7,
        "avg: %.2f \nstd: %.2f" % (avg_duration, stddev_duration),
    )

g.add_legend()
g.savefig("output.png")
