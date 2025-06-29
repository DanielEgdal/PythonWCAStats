from duckdb_reader import duckdb_reader


db = duckdb_reader(['results','persons'])

query = """
with cte as (
select person_id, sum(case when regional_single_record is not null then 1 else 0 end)+sum(case when regional_average_record is not null then 1 else 0 end) gts
from results
where country_id = 'Denmark'
group by person_id
)

select c.gts, p.name
from cte c
inner join persons p
on c.person_id = p.wca_id
where p.sub_id = 1 and c.gts > 0
order by c.gts desc

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