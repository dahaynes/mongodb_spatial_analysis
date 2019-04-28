h = db.states_hash_2.mapReduce( 
    function() { 
        emit(this.NAME, this.geom);
    },
    function(key, values){
        r = db.points.aggregate(
           [{
               "$match": {
                    "geom": {
                       $geoIntersects: {
                          $geometry: { values
//                               type: "Polygon",
//                               coordinates: [[
//                               [ -105.20746339,  36.992426 ],
//                               [ -105.20746339,  41.003444 ],
//                               [-102.041524,  41.003444],
//                               [-102.041524,  36.992426],
//                               [ -105.20746339,  36.992426 ]
//                               ]]
                              
                              //"BOX(-109.060253 36.992426,-102.041524 41.003444)"
                              //"BOX(-111.056888 40.994746,-104.05216 45.005904)"
                            }
                        }
                    }   
               }
               
           },
           {
               "$count" : "num_points"   }
           ]
        )
        return Array.sum(r.num_points)
        },
    {
        query: { NAME: "Colorado"},
        out: "total_points"
        }
//         k = this.geom;
//         print("the geom" + k.toString());
//         }
    );
        
 