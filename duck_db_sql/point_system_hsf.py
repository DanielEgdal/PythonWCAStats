from duckdb_reader_v2 import duckdb_reader


db = duckdb_reader(['Results','Persons','Competitions'])

query = """
with cte as (
select personname, competitionid,
case pos when 1 then 25
        when 2 then 18
        when 3 then 15
        when 4 then 12
        when 5 then 10
        when 6 then 8
        when 7 then 6
        when 8 then 4
        when 9 then 2
        when 10 then 1
        else 0 end as tst
from results r
join competitions c
on c.id = r.competitionid
where r.roundtypeid in ('c','f') and best > 0
)

select personname, sum(tst) as point
from cte
where competitionid in ('HvidovreLigaI2025','HvidovreLigaII2025')
group by personname
order by point desc

"""


r = db.do_query(query)

print(r)

r.to_csv('tmp.csv')
