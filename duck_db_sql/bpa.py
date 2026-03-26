from duckdb_reader_v2 import duckdb_reader


db = duckdb_reader(['result_attempts','results'])

query = """
with cte as (
select result_id, competition_id, coalesce(nullif(value,-1),1000000) as value, attempt_number, best, row_number() over(partition by result_id order by coalesce(nullif(value,-1),1000000),attempt_number) rn
from result_attempts ra
join results r
on r.id = ra.result_id
where person_id = '2016KELL10'
and event_id = '555'
and attempt_number != 5)

select competition_id, result_id, sum(value)/300 bpa
from cte
where rn < 4
group by competition_id, result_id
order by bpa
"""

# select person_name, c.gts
# from results r
# join cte c
# on c.person_id = c.person_id

r = db.do_query(query)

print(r)

r.to_csv('tmp.csv')

# select cd.competition_id, cd.wca_id
# from comp_delegates cd
# where not exists (select 1 
#                 from comp_competitors
#                     where comp_competitors.personid = cd.wca_id and comp_competitors.competitionid = cd.competition_id)