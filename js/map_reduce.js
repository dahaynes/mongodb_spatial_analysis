// *** 3T Software Labs, Studio 3T: MapReduce Job ****

// Variable for map
var __3tsoftwarelabs_map = function() { 
        emit(this.NAME, {geom: this.geom, points: 0});
    };

// Variable for reduce
var __3tsoftwarelabs_reduce = function(key, value){	var results = db.randompoints_hashed.aggregate(		[			{			    "$match": {			        "geom": {			            $geoIntersects: {			                $geometry: polyGeom			            }			        }			    }			},			{			    "$count": "num_points"			}		]	)	//j = points.toArray()[0]	//this.value.records = "$$points.count"	//printjson(j)	//db.states_hashed.insert(points.pretty)	//var doc = points.pretty()	var pointCount = results._batch[0].num_points	//print(key, pointCount)	value.points.points = pointCount	//db.states_hashed.insert({points: pointCount})	return(pointCount)
};

db.runCommand({ 
    mapReduce: "states_hashed",
    map: __3tsoftwarelabs_map,
    reduce: __3tsoftwarelabs_reduce,
    out: { "reduce" : "", "sharded" : false, "nonAtomic" : false },
    inputDB: "research",
 });
