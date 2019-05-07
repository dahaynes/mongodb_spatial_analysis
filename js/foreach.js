db.getCollection("states_hashed").forEach( function(poly) {
	theGeom = poly.geom;
	print(theGeom);
};
