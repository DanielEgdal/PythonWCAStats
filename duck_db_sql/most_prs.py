from duckdb_reader_v2 import duckdb_reader


db = duckdb_reader(['results','competitions'])

query = """
with pr_at_date_avg as (
select person_id, c.id, make_date(c.year, c.end_month, c.end_day) as end_date, r.event_id, round_type_id, min(average) OVER (PARTITION BY person_id,r.event_id ORDER BY end_date, round_type_id) as pr_at_time, average
from results r
join competitions c
on c.id = r.competition_id
where r.person_country_id = 'Denmark' and average > 0)

, all_prs_avg as(
select person_id, id as comp, event_id, round_type_id, pr_at_time as pr
from pr_at_date_avg
where average = pr_at_time)

, pr_at_date_single as (select person_id, c.id, make_date(c.year, c.end_month, c.end_day) as end_date, r.event_id, round_type_id, min(best) OVER (PARTITION BY person_id, r.event_id ORDER BY end_date, round_type_id) as pr_at_time, best as single
from results r
join competitions c
on c.id = r.competition_id
where r.person_country_id = 'Denmark' and best> 0)

, all_prs_single as(
select person_id, id as comp, event_id, round_type_id, pr_at_time as pr
from pr_at_date_single 
where single = pr_at_time ) 

, all_prs as(
select*
from all_prs_single 
union
select*
from all_prs_avg )

select person_id, count(*) gts
from all_prs 
group by person_id 
order by gts desc
"""

r = db.do_query(query)

print(r)

r.to_csv('tmp.csv')