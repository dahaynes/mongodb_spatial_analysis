
    


function centroidOfPolyFeature(polyCollection) {
var startTime = new Date().getTime();
var mongoCur = db.getCollection(polyCollection).find({})
while (mongoCur.hasNext()) {
    var f = mongoCur.next()
    featureGeomType = f.geom.type
    if (featureGeomType == 'Polygon') {
        
    		print(f.NAME," is a polygon");
    		var thePolygon = polygon(f.geom.coordinates,{name: f.NAME});
			printjson(centroid(thePolygon));
    } else if (featureGeomType == 'MultiPolygon') {

    		print("mmulti polygon", f.NAME);
    		var theMultiPolygon = multiPolygon(f.geom.coordinates, {name: f.NAME});
    		printjson(centroid(theMultiPolygon));
    		
    }
    print(featureGeomType);


    
	}
var stopTime = new Date().getTime();
print("Elapsed Time: ", stopTime-startTime);
}



//centroidOfPolyFeature("states_hashed")

