db.getCollection('two_states').find().forEach( function(state) {
    k = state.geom
    print( "user: " + state.geom ); 
    
    r = db.points.aggregate(
   [{
       "$match": {
            "geom": {
               $geoIntersects: {
                  $geometry: { k
//                       type: "Polygon",
//                       coordinates: [[
//                       [ -105.20746339,  36.992426 ],
//                       [ -105.20746339,  41.003444 ],
//                       [-102.041524,  41.003444],
//                       [-102.041524,  36.992426],
//                       [ -105.20746339,  36.992426 ]
//                       ]]
                      
                      //"BOX(-109.060253 36.992426,-102.041524 41.003444)"
                      //"BOX(-111.056888 40.994746,-104.05216 45.005904)"
                        }
                    }
                }   
           }
           
       },
       {
        "$count" : "ROW_NUMBER"   
       }]
    )
    print( "user: " + r.pretty() );  
      
   }
 );

//db.users.find().forEach( function(myDoc) { print( "user: " + myDoc.name ); } );