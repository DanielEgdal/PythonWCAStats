from duckdb_reader_v2 import duckdb_reader


db = duckdb_reader(['results','competitions','round_types'])

event_id = 'skewb'
placement = 1

query = f"""
with tmp as (
select person_id, competition_id
, make_date(c.year, c.end_month, c.end_day) as end_date
, event_id, pos, rt.rank as rt_rank
, rank() over(partition by person_id, event_id order by person_id, end_date, event_id, rt.rank) runde_nummer
from results r
join competitions c
on c.id = r.competition_id
left join round_types as rt
on r.round_type_id = rt.id
where event_id = '{event_id}')
,
runde_diff as (
select person_id, competition_id, end_date, event_id, pos, rt_rank, runde_nummer
, lag(runde_nummer, 1, 0) over (order by person_id, runde_nummer) tidligere_runde 
, row_number() over(order by person_id, end_date, event_id, rt_rank) as rækkenummer
from tmp
where pos = {placement})
,
ongoing as (
select person_id, competition_id, end_date, event_id, pos, rt_rank, runde_nummer, rækkenummer,
case when runde_nummer-1 = tidligere_runde then true
when rækkenummer = 1 then true
else false end as on_going_streak
from runde_diff)
,
streak_groups as (
select person_id, competition_id, end_date, event_id, pos, rt_rank, runde_nummer, rækkenummer, on_going_streak,
SUM(CASE WHEN on_going_streak = 0 THEN 1 ELSE 0 END) OVER (ORDER BY person_id, end_date, event_id, rt_rank) streak_group
from ongoing
order by runde_nummer)

select person_id, event_id, MIN(END_DATE) started_on, MAX(END_DATE) ended_on, count(*) gts
from streak_groups
group by person_id, event_id, streak_group
order by gts desc

"""

r = db.do_query(query)

print(r)

r.to_csv('tmp.csv')