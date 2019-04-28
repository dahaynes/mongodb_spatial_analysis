db.points.mapReduce( 
    function() { 
        emit(this.HASH_2, 1);
    },
    function(key, values){
        return Array.sum(values)
        },
    {
        query: { HASH_2: "9x"},
        out: "total_hash"
        }
//         k = this.geom;
//         print("the geom" + k.toString());
//         }
    )