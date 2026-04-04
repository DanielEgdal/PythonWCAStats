from duckdb_reader_v2 import duckdb_reader
from duckdb_extensions import import_extension # https://pypi.org/project/duckdb-extension-spatial/

db = duckdb_reader(['results','competitions','persons'])
import_extension("spatial",con=db.conn)

# ST_Distance_Spheroid, is apparently the best
# ST_Distance_Sphere

query = """
LOAD spatial;
with comps as (
SELECT distinct
person_id, r.person_country_id, c.id, make_date(c.year, c.end_month, c.end_day) as comp_date
, ST_Point(c.latitude_microdegrees/1_000_000, c.longitude_microdegrees/1_000_000) as comp_point
from results r
join competitions c
on c.id = r.competition_id
where left(c.country_id,1) != 'X' 
and r.person_country_id = 'Denmark'
)

, comps_ranked as (
select c.*
, row_number() over(PARTITION BY person_id order by comp_date) comp_rnk
from comps c)

, distances as (
select c1.person_id, c1.comp_rnk, c2.comp_rnk, ST_Distance_Spheroid(c1.comp_point, c2.comp_point)/1000 as dist_km
from comps_ranked c1
join comps_ranked c2
on c1.person_id = c2.person_id
and c1.comp_rnk+1 = c2.comp_rnk
where c1.comp_point != c2.comp_point
)

, summed_distances as (
select person_id, sum(dist_km) distance_summed
from distances
group by person_id
)

select p.name, s.*
from summed_distances s
join persons p
on p.wca_id = s.person_id
and p.sub_id = 1
order by distance_summed desc

"""

# ST_Point

r = db.do_query(query)

print(r)

r.to_csv('tmp.csv')
