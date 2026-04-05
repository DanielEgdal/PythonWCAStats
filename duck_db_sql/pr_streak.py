from duckdb_reader_v2 import duckdb_reader


db = duckdb_reader(['results','competitions','round_types'])

query = f"""
with pr_at_date_avg as (
select person_id, c.id, make_date(c.year, c.end_month, c.end_day) as end_date
, r.event_id, rt.rank as rt_rank
, min(average) OVER (PARTITION BY person_id,r.event_id ORDER BY end_date, rt_rank) as pr_at_time, average
from results r
join competitions c
on c.id = r.competition_id
left join round_types as rt
on r.round_type_id = rt.id
where r.person_country_id = 'Denmark' and average > 0)

, all_prs_avg as(
select person_id, id as comp, event_id, rt_rank, pr_at_time as pr
from pr_at_date_avg
where average = pr_at_time)

, pr_at_date_single as (select person_id, c.id, make_date(c.year, c.end_month, c.end_day) as end_date
, r.event_id, rt.rank as rt_rank
, min(best) OVER (PARTITION BY person_id, r.event_id ORDER BY end_date, rt_rank) as pr_at_time, best as single
from results r
join competitions c
on c.id = r.competition_id
left join round_types as rt
on r.round_type_id = rt.id
where r.person_country_id = 'Denmark' and best> 0)

, all_prs_single as(
select person_id, id as comp, event_id, rt_rank, pr_at_time as pr
from pr_at_date_single 
where single = pr_at_time ) 

, all_prs as(
select*
from all_prs_single 
union
select*
from all_prs_avg )

, comps_ordered as (
select r.person_id, c.id as comp, make_date(c.year, c.end_month, c.end_day) as end_date
, RANK() OVER (PARTITION BY person_id ORDER BY end_date, c.id) as comp_number
from (select distinct person_id, competition_id 
    from results) as r
join competitions c
on c.id = r.competition_id

)

, pr_overview as (
select co.person_id, co.comp, co.end_date, comp_number, case when a.comp is null then false else true end as pr_at_comp_flag
from comps_ordered co
left join (select distinct person_id, comp from all_prs) as a
on co.person_id = a.person_id
and co.comp = a.comp
-- where co.person_id = '2013EGDA01'
order by co.comp_number)

, streak_group as (
select *,
SUM(CASE WHEN pr_at_comp_flag = false THEN 1 ELSE 0 END) OVER (PARTITION BY person_id ORDER BY person_id, comp_number) streak_group
from pr_overview)

select person_id
, min(end_date) first_comp_with_pr_date, min_by(comp,end_date) first_comp_with_pr
, max(end_date) last_comp_with_pr_date, max_by(comp,end_date) last_comp_with_pr
, count(*) as comps_with_pr
from streak_group
where pr_at_comp_flag = true
group by streak_group, person_id
order by comps_with_pr desc

"""

r = db.do_query(query)

print(r)

r.to_csv('tmp.csv')