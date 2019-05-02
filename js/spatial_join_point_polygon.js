db.getCollection("states_hashed").aggregate(

	// Pipeline
	[
		// Stage 1
		{
			$lookup: // Equality Match
			{
			    from: "randompoints_hashed",
			    localField: "HASH_2",
			    foreignField: "HASH_2",
			    as: "points"
			}
			
			// Uncorrelated Subqueries
			// (supported as of MongoDB 3.6)
			// {
			//    from: "<collection to join>",
			//    let: { <var_1>: <expression>, …, <var_n>: <expression> },
			//    pipeline: [ <pipeline to execute on the collection to join> ],
			//    as: "<output array field>"
			// }
		},

		// Stage 2
		{
			$unwind: {
			    path : "$points",
			    includeArrayIndex : "arrayIndex", // optional
			    preserveNullAndEmptyArrays : false // optional
			}
		},

		// Stage 3
		{
			$project: {
			     _id: "0", NAME: 1, geom: "1", points_x: {geom: {coordinates: {$toDouble: {$arrayElemAt: ["$points.geom.coordinates", 0] } }}}, points_y: {geom: {coordinates: {$toDouble: {$arrayElemAt: ["$points.geom.coordinates", 1] } }}}
			//     "$points.geom.coordinates(0)
			}
		},

		// Stage 4
		{
			$match: {
			"geom" : {
			    "$geoIntersects" : {
			        "$geometry" : {
			            //"$points.geom.coordinates(0)"
			            "type": "Point",
			//            "coordinates": [-100.19417611867914, 39.39287051366615]
			            "coordinates": ["$points_x.geom.coordinates", "$points_y.geom.coordinates"]
			        	}
			    	}
				}
			}
		},

		// Stage 5
		{
			$group: {
			 _id: {"NAME": "NAME"}, count: {$sum: 1}
			}
		},

	]

	// Created with Studio 3T, the IDE for MongoDB - https://studio3t.com/

);
