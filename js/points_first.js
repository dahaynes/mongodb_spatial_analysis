db.getCollection('points').find().forEach( function(pt) {
    k = pt.geom.coordinates;
    y =  parseFloat(k.pop(0))
    x =  parseFloat(k)
    print( "user: " ); 


    r = db.two_states.aggregate(
   [{
       "$match": {
            "geom": {
               $geoIntersects: {
                  $geometry: { 
                      type: "Point",
                      coordinates: [x,y]
//                       coordinates: [[
// //                       [ -105.2074639,  36.992426 ],
// //                       [ -105.20746339,  41.003444 ],
// //                       [-102.041524,  41.003444],
// //                       [-102.041524,  36.992426],
// //                       [ -105.20746339,  36.992426 ]
// //                       ]]
                      
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
    //print( "user: " + r.pretty() );  
     
   }
 );

//db.users.find().forEach( function(myDoc) { print( "user: " + myDoc.name ); } );