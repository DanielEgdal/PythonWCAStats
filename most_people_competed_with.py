import polars as pl

res_path = '../wca_export/WCA_export_Results.tsv'

res = pl.read_csv(res_path, sep='\t').lazy()

res_filtered = res.select(['competitionId', 'personId']).unique()

res_joined = res_filtered.join(res_filtered, on="competitionId", suffix="_2").select(['personId', 'personId_2']).unique()

competitors_seen = res_joined.groupby("personId").agg(
    pl.col('personId_2').count().alias('count')
)

df = competitors_seen.collect().to_pandas()
df['count'] = df['count']-1 # Do not count oneself

df = df.sort_values('count',ascending=False)

print(df.head(20))