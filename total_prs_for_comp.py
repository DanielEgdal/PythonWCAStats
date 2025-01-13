from duckdb_reader import duckdb_reader


db = duckdb_reader(['Competitions',"Results"]) # Note, ideally this should also use the "RoundTypes" table

query = """
with pr_at_date_avg as (select personid, c.id, c.end_date, r.eventid, roundtypeid, min(average) OVER (PARTITION BY personid, r.eventid ORDER BY c.end_date, roundtypeid) as pr_at_time, average
from Results r
join Competitions c
on c.id = r.competitionid
where average > 0)

, all_prs_avg as(
select personid, id as comp, eventid, roundtypeid, pr_at_time as pr
from pr_at_date_avg
where average = pr_at_time)

, pr_at_date_single as (select personid, c.id, c.end_date, r.eventid, roundtypeid, min(best) OVER (PARTITION BY personid, r.eventid ORDER BY c.end_date, roundtypeid) as pr_at_time, best as single
from Results r
join Competitions c
on c.id = r.competitionid
where best> 0)

, all_prs_single as(
select personid, id as comp, eventid, roundtypeid, pr_at_time as pr
from pr_at_date_single 
where single = pr_at_time ) 

, all_prs as(
select*
from all_prs_single 
union
select*
from all_prs_avg )

select comp, count(*) gts
from all_prs 
group by comp 
order by gts desc

"""

r = db.do_query(query)

print(r)

r.to_csv('tmp.csv')