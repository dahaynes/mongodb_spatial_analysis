db.two_states.aggregate(
    [
        { $lookup:
            {   from: "points",
                localField: "HASH_2",
                foreignField: "HASH_2",
                as: "points"
            }  
        },
        { 
            $unwind: "$points"
        }
//         ,{
//         $group: {
//                     _id: "HASH_2", count: { $sum: 1 }
//                 }
//             }
//         ,{
//             "$match": {
//                 "geom": {
//                     $geoIntersects: {
//                         $geometry: {
//                                 tojsonObject("$points.geom")
// //                             "type": "Point",
// //                             "coordinates": [parseFloat("$points.geom.coordinates[0]"),parseFloat("$points.geom.coordinates[1]")] //[-107.862889172811, 38.8613591309177]
//                             }
//                         }
//                     }
//                 }
//         }
    
    ]
)