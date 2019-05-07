// *** 3T Software Labs, Studio 3T: MapReduce Job ****

// Variable for db
var __3tsoftwarelabs_db = "research";

// Variable for map
var __3tsoftwarelabs_map = function () {

    // Enter the JavaScript for the map function here
    // You can access the current document as 'this'
    //
    // Available functions: assert(), BinData(), DBPointer(), DBRef(), doassert(), emit(), gc()
    //                      HexData(), hex_md5(), isNumber(), isObject(), ISODate(), isString()
    //                      Map(), MD5(), NumberInt(), NumberLong(), ObjectId(), print()
    //                      printjson(), printjsononeline(), sleep(), Timestamp(), tojson()
    //                      tojsononeline(), tojsonObject(), UUID(), version()
    //
    // Available properties: args, MaxKey, MinKey

    emit(this.NAME, this.geom);
}
;

// Variable for reduce
var __3tsoftwarelabs_reduce = function (key, value) {

    // Enter the JavaScript for the reduce function here
    // 'values' is a list of objects as emit()'ed by the map() function
    // Make sure the object your return is of the same type as the ones emit()'ed
    //
    // Available functions: assert(), BinData(), DBPointer(), DBRef(), doassert(), emit(), gc()
    //                      HexData(), hex_md5(), isNumber(), isObject(), ISODate(), isString()
    //                      Map(), MD5(), NumberInt(), NumberLong(), ObjectId(), print()
    //                      printjson(), printjsononeline(), sleep(), Timestamp(), tojson()
    //                      tojsononeline(), tojsonObject(), UUID(), version()
    //
    // Available properties: args, MaxKey, MinKey
	
	var reducedValue = db.randompoints_hashed.aggregate(
		[
			"$match": {
			    "geom" : {
			        $geometry: {value
			        }
			    }
			}
			,{
			    "$count": "num_points"
			}
		]
	);
    //var reducedValue = "" + values;

    return reducedValue;
}
;

db.runCommand({ 
    mapReduce: "states_hashed",
    map: __3tsoftwarelabs_map,
    reduce: __3tsoftwarelabs_reduce,
    out: { "inline" : 1 },
    query: { },
    sort: { },
    inputDB: "research",
 });
