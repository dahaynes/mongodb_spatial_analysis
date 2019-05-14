function pointPolygonCount(polyCollection, pointCollection) {

var polyCursor = db.getCollection(polyCollection).find();
var startTime = new Date().getTime();
while (polyCursor.hasNext()) {
    var poly = polyCursor.next()
    var results = db.getCollection(pointCollection).aggregate(
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
var stopTime = new Date().getTime();
print("Elapsed Time: ", stopTime-startTime)
}

pointPolygonCount("states_hashed", "randompoints_10m_hashed")
