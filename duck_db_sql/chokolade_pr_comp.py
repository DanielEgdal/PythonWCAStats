from duckdb_reader_v2 import duckdb_reader


db = duckdb_reader(['result_attempts','results','competitions'])

query = """
with cte as (
select r.competition_id,r.person_id, ra.value
from result_attempts ra
join results r
on r.id = ra.result_id
where ra.value < 10*60*100 and ra.value > 0
)

, comp_dates as (
select id, make_date(c.year,c.month,c.day) comp_date, country_id
from competitions c
)

, grouped_counts as (
SELECT competition_id,
       SUM(IF(value % 100 = 0, 1, 0)) AS chok_count,
       COUNT(*) AS total_solves
FROM cte
GROUP BY competition_id
)

select competition_id, chok_count, total_solves, chok_count/total_solves as ratio, comp_date
from grouped_counts g
join comp_dates c
on c.id = g.competition_id
where country_id = 'Denmark'
and year(comp_date) > 2024
order by ratio desc
"""

r = db.do_query(query)

print(r)

r.to_csv('tmp.csv')

# select cd.competition_id, cd.wca_id
# from comp_delegates cd
# where not exists (select 1 
#                 from comp_competitors
#                     where comp_competitors.personid = cd.wca_id and comp_competitors.competitionid = cd.competition_id)