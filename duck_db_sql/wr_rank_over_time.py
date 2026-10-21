from duckdb_reader_v2 import duckdb_reader
import matplotlib.pyplot as plt


db = duckdb_reader(['results','competitions','round_types'])
event = 'clock'
wcaid = '2013EGDA01'

query = f"""
WITH date_results AS (
    SELECT DISTINCT 
        r.person_id,
        make_date(end_year,end_month,end_day) AS end_date,
        MIN(average) OVER (
            PARTITION BY r.person_id 
            ORDER BY make_date(end_year,end_month,end_day)
        ) AS PR_at_time
    FROM competitions c
    JOIN results r
        ON r.competition_id = c.id
    WHERE event_id = '{event}'
      AND average > 0
),

history AS (
    SELECT
        *,
        LEAD(end_date, 1, DATE '9999-12-31') OVER (
            PARTITION BY person_id
            ORDER BY end_date
        ) AS next_date
    FROM date_results
)

SELECT
    me.end_date,
    me.PR_at_time/100 as PR_at_time,
    1 + COUNT(other.person_id) AS world_rank
FROM history me
LEFT JOIN history other
    ON other.person_id <> me.person_id
    AND other.end_date <= me.end_date
    AND me.end_date < other.next_date
    AND other.PR_at_time < me.PR_at_time
WHERE me.person_id = '{wcaid}'
GROUP BY
    me.end_date,
    me.PR_at_time
ORDER BY
    me.end_date;

"""

r = db.do_query(query)

df = r.to_df()

df = df[df['end_date'] > '2014-01-01']

# print(r)

plt.figure(figsize=(12, 6))

# World rank line
plt.plot(
    df['end_date'],
    df['world_rank'],
    label='World rank'
)

# Only show PR improvement points
pr_changes = df[df['PR_at_time'].diff() != 0]

plt.scatter(
    pr_changes['end_date'],
    pr_changes['world_rank'],
    label='New PR'
)

# Add PR values next to points
for _, row in pr_changes.iterrows():
    plt.annotate(
        f"{row['PR_at_time']:.2f}",
        (row['end_date'], row['world_rank']),
        xytext=(5, 5),
        textcoords='offset points'
    )

# Normal y-axis (0 at bottom)
plt.ylim(bottom=0)

# Grid
plt.grid(True)

# Labels
plt.xlabel('Date')
plt.ylabel('World rank')
plt.title('World rank progression with PR improvements')

# Legend outside plot
plt.legend(
    bbox_to_anchor=(1.05, 1),
    loc='upper left'
)

# Make room for legend
plt.tight_layout()

plt.show()