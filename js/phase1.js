db.getCollection("states_hashed").aggregate(

	// Pipeline
	[
		// Stage 1
		{
			$lookup: {
			    from: "randompoints_hashed",
			    localField: "HASH_2",
			    foreignField: "HASH_2",
			    as: "points"
			}
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
			     _id: 0, NAME: 1, geom: 1, "points.geom": 1, points_x: {geom: {coordinates: {$toDouble: {$arrayElemAt: ["$points.geom.coordinates", 0] } }}}, "points_y.geom.coordinates": {$toDouble: {$arrayElemAt: ["$points.geom.coordinates", 1] } }
			//     "$points.geom.coordinates(0)
			}
		},

		// Stage 4
		{
			$out: "point_n_polys"
		},

	]

	// Created with Studio 3T, the IDE for MongoDB - https://studio3t.com/

);
