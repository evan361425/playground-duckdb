from typing import Any
import pandas as pd
import duckdb as duck
from pandas.core.frame import DataFrame
import seaborn as sns

from constant import GROUP_PREFIXES, IMAGES_FOLDER, URL_PREFIX


conn = duck.connect("data/prod.db")


def getMetadata(query: str, groupBy: str = "url_clean_path"):
    return pd.read_sql(
        """
    SELECT * FROM (
        SELECT
            %s,
            avg(duration) AS avg_duration,
            max(duration) AS max_duration,
            min(duration) AS min_duration,
            stddev(duration) AS stddev_duration,
            count(*) AS data_count 
        FROM xray
        WHERE url_prefix = '%s'
        AND url_clean_path %s
        GROUP BY %s
    ) AS t
    ORDER BY avg_duration DESC
    """
        % (groupBy, URL_PREFIX, query, groupBy),
        conn,
    )


def getDetail(query: str):
    print("  - getting details")
    return pd.read_sql(
        """
        SELECT duration, http_status, url_clean_path
        FROM xray
        WHERE url_prefix = '%s'
        AND %s"""
        % (URL_PREFIX, query),
        conn,
    )


def drawSmallDf(df: pd.DataFrame, name: str):
    metadata = df.to_dict(orient="list")
    query = "url_clean_path IN ('%s')" % ("','".join(metadata["url_clean_path"]))

    drawDataframe(metadata=metadata, details=getDetail(query), filename=name)


def getPalette():
    return {
        200: "green",
        302: "blue",
        400: "yellow",
        401: "gold",
        403: "yellow",
        404: "yellow",
        500: "red",
        504: "black",
    }


def drawDataframe(
    metadata: dict[str, Any],
    details: pd.DataFrame,
    filename: str,
    col: str = "url_clean_path",
):
    dataSize = len(metadata[col])
    if dataSize == 0:
        return

    print("  - setup painting")
    useHue = col == "url_clean_path"
    g = sns.displot(
        data=details,
        x="duration",
        hue="http_status" if useHue else None,
        kind="hist",
        legend=useHue,
        col=col,
        col_wrap=dataSize if dataSize < 3 else 3,
        col_order=metadata[col],
        aspect=2,
        multiple="stack" if useHue else "layer",
        palette=getPalette(),
        kde=True,
        facet_kws={
            "sharex": not useHue,
            "sharey": False,
        },
    )
    g.set_titles(col_template="{col_name}")

    # Draw Avg and Stddev
    print("  - setup limit")

    if col == "http_status":
        index = metadata[col].index(200)
        avg_duration = metadata["avg_duration"][index]
        stddev_duration = metadata["stddev_duration"][index]
        global_x_min = max(avg_duration - 2 * stddev_duration, 0)
        global_x_max = avg_duration + 2 * stddev_duration
    else:
        global_x_min = None
        global_x_max = None

    for (
        key,
        avg_duration,
        stddev_duration,
        data_count,
        max_duration,
        min_duration,
    ) in zip(
        metadata[col],
        metadata["avg_duration"],
        metadata["stddev_duration"],
        metadata["data_count"],
        metadata["max_duration"],
        metadata["min_duration"],
    ):
        ax = g.axes_dict[key]

        # draw average
        ax.axvline(avg_duration, color="k", linestyle="dashed", linewidth=1)

        if global_x_min is None:
            x_min = max(avg_duration - 2 * stddev_duration, 0)
            x_max = avg_duration + 2 * stddev_duration
            if x_min == x_max:
                x_min = max(0, x_min - 0.1)
                x_max = x_max + 0.1
            ax.set_xlim(x_min, x_max)
        else:
            ax.set_xlim(global_x_min, global_x_max)
            x_min, x_max = (global_x_min, global_x_max)

        y_min, y_max = ax.get_ylim()
        ax.text(
            x_min + (x_max - x_min) * 0.7,
            y_min + (y_max - y_min) * 0.7,
            "avg: %.2f \nstd: %.2f\nnum: %d\nmax: %.2f\nmin: %.2f"
            % (avg_duration, stddev_duration, data_count, max_duration, min_duration),
        )

    g.tight_layout()
    g.savefig("images/%s/%s.png" % (IMAGES_FOLDER, filename))


# SELECT grp FROM (
#   SELECT string_split(url_clean_path,'/')[0] AS grp FROM xray
#   WHERE url_prefix = '$URL_PREFIX'
# ) AS t GROUP BY grp ORDER BY grp ASC;
for group in GROUP_PREFIXES:
    groupName = group.rstrip("/")
    print("Start process '%s'" % groupName)

    df = getMetadata("LIKE '%s%%'" % group)

    smallDf = df[df.data_count < 10000]
    bigDf = df[df.data_count >= 10000]

    if bigDf.empty:
        drawSmallDf(smallDf, groupName)
        continue

    if not smallDf.empty:
        drawSmallDf(smallDf, "%s-small" % groupName)

    # continue

    for path in bigDf["url_clean_path"].tolist():
        print("  - splitting %s" % path)
        metadata = getMetadata("= '%s'" % path, "http_status").to_dict(orient="list")
        details = getDetail("url_clean_path = '%s'" % path)
        filename = "-".join(path.split("/"))

        drawDataframe(metadata, details, filename, col="http_status")
