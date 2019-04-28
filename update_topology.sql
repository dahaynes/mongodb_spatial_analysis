-- alter table tracts2 add geom2 geometry;
update tracts2
SET geom2 = ST_simplifyPreserveTopology(geom, .001)

SELECT ST_AsText(geom)
FROM tracts2 

SELECT gid, namelsad00, ST_IsValid(geom), geom
FROM tracts2
LIMIT 10
WHERE ST_IsValid(geom)

-- 64882