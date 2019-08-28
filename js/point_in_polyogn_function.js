 db.system.js.save(     
    { _id : "pointPolygonCount" , value : function (polyCollection, pointCollection){
      var polyCursor = db.getCollection(polyCollection).find();
      var startTime = new Date().getTime();
      var resultsDict = {};
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

     if (results._batch.length > 0){
              var points = results._batch[0].num_points;
              //print(poly.NAME, points);
              resultsDict[poly.place_name] = points;
        } else {
              //print(poly.NAME, 0);
           resultsDict[poly.place_name] = 0;
        } 
    }
  var stopTime = new Date().getTime();
  print("Elapsed Time: ", stopTime-startTime);
  return resultsDict;          }    } );