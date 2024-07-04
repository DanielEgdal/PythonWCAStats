import polars as pl
import pandas as pd

def get_bpa(data):

    am_invalid = sum(
        (pl.col(f"value{i}") == 0).cast(pl.Int64) for i in range(1, 5))

    solves_sum = sum(pl.col(f"value{i}") for i in range(1, 5))
    # best_solve = pl.min_horizontal(pl.col(f"value{i}") for i in range(1, 5))
    worst_solve = pl.max_horizontal(pl.col(f"value{i}") for i in range(1, 5))

    data = data.with_columns(
        pl.when(am_invalid > 0)
        .then(pl.lit(None))
        .otherwise((((solves_sum - worst_solve)) / 3).round().cast(pl.Int64))
        .alias('bpa')
    )
    return data

def load_results_date():
    comps_path = '../wca_export/WCA_export_Competitions.tsv'
    res_path = '../wca_export/WCA_export_Results.tsv'

    comps = (pl.read_csv(comps_path,separator='\t')).lazy()
    res = pl.read_csv(res_path,separator='\t').lazy()
    person_res = comps.join(res,left_on='id',right_on='competitionId')

    person_res = person_res.with_columns(
        pl.datetime(
            pl.col('year'), 
            pl.col('endMonth'), 
            pl.col('endDay')
        ).alias('date')
    )

    DNF_VAL = 99999999999
    person_res = person_res.lazy().with_columns([
        (pl.when(pl.col(f"value{i}") < 0)
        .then(DNF_VAL)
        .otherwise(pl.col(f"value{i}"))
        .alias(f"value{i}"))
        for i in range(1, 5)
        ])

    person_res = get_bpa(person_res)

    person_res = person_res.select(["personId","eventId","date","average","bpa"])
    return person_res

def old_get_wr_by_date(data):
    # This way was 6x slower than below
    possible_wr = data.filter(pl.col('average')>0).group_by(["eventId","date"]).agg(
        pl.col('average').min().alias("possible_wr")
    )

    possible_wr = possible_wr.sort(['eventId','date'])

    def remove_non_wrs(df):
        df = df.with_columns(
            pl.col('possible_wr').shift(1).alias('prev_value'),
            pl.col('eventId').shift(1).alias('prev_event')
        )
        filtered_df = df.filter((pl.col('possible_wr') <= pl.col('prev_value')) | pl.col('prev_value').is_null() | (pl.col('prev_event') != pl.col('eventId')))
        return filtered_df.drop(['prev_value','prev_event'])
    
    prev_am_rows = -1
    am_rows = possible_wr.select(pl.len()).collect().select('len').item()

    while prev_am_rows != am_rows:
        print(prev_am_rows,am_rows)
        prev_am_rows = am_rows
        possible_wr = remove_non_wrs(possible_wr)
        am_rows = possible_wr.select(pl.len()).collect().select('len').item()

    return possible_wr

def get_wr_by_date(data):
    possible_wr = data.filter(pl.col('average') > 0).group_by(["eventId", "date"]).agg(
        pl.col('average').min().alias("possible_wr")
    )
    
    possible_wr = possible_wr.sort(['eventId', 'date']).collect()

    valid_rows = []

    previous_value = None
    previous_event = None
    
    for row in possible_wr.iter_rows(named=True):
        current_value = row['possible_wr']
        current_event = row['eventId']
        
        if previous_event != current_event or previous_value is None or current_value <= previous_value:
            valid_rows.append(row)
            previous_value = current_value
            previous_event = current_event
    
    filtered_df = pl.DataFrame(valid_rows)

    return filtered_df

def see_wr_at_time_of_result():
    data = load_results_date()
    wrs = get_wr_by_date(data).lazy()

    data = data.filter(pl.col('bpa').is_not_null())

    data = data.with_row_index()

    joined_df = data.join(
        wrs,
        # on=(
        #     (pl.col('date') <= wrs.select('date')) &
        #     (pl.col('eventId') == wrs.select('eventId')),
        # ),
        left_on='eventId',
        right_on='eventId',
        suffix='_wr'
    )

    joined_df = joined_df.filter(pl.col('date') >= pl.col('date_wr'))

    max_dates = joined_df.group_by('index').agg(
        pl.col('date_wr').max().alias('max_date_wr')
    )

    merged_df = joined_df.join(max_dates, on='index')

    df = merged_df.filter(
        pl.col('date_wr') == pl.col('max_date_wr'))

    df = df.filter(pl.col('bpa')<= pl.col('possible_wr'))


    return df

df = see_wr_at_time_of_result().collect().to_pandas()

df = df[df['date']!=df['date_wr']] # Remove the actual instances of WRs

wr_chances_by_person = pd.DataFrame(df.groupby('personId')['index'].count().sort_values(ascending=False))

print(wr_chances_by_person)
