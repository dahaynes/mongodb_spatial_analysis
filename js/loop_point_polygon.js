

//k = db.eval( function(key, polyGeom){
//	// var r = values.geom.type

var polyCursor = db.states_hashed.find();
// poly = polyCursor.next()
while (polyCursor.hasNext()) {
    var poly = polyCursor.next()
    var results = db.randompoints_hashed.aggregate(
	[
		{
		    "$match": {
		        "geom": {
		            $geoIntersects: {
		                $geometry: poly.geom
		            }
		        }
		    }
		},
		{
		    "$count": "num_points"
		}
	]);
	var points = results._batch[0].num_points;
	print(poly.NAME, points);
}



	//points.pretty()

	

//var x = db.states_hashed.findOne({"NAME": "Colorado"});
//
//var results = db.randompoints_hashed.aggregate(
//		[
//			{
//			    "$match": {
//			        "geom": {
//			            $geoIntersects: {
//			                $geometry: x.geom
//			            }
//			        }
//			    }
//			},
//			{
//			    "$count": "num_points"
//			}
//		]
//	);



//results = {points}
//print(x.NAME)
//	points.pretty()
//
//points.forEach()
//
//points.
//results.points.pretty()

//var pointCount = results._batch[0].num_points
//db.states_hashed.insert({points: pointCount})
//
//
//print(x.NAME, points)
//points.returnKey