//-------------------------------------
// 1.1
//-------------------------------------
db.airlines.aggregate(
	[
		{
			$group: {
				_id: "$class",
				total: { $sum: 1 }
			}
		},
		{
			$project: {
			    _id : 0,
			    class: "$_id",
			    total: "$total"
			}
		}
	]
);
// Result:
// { "class" : "F", "total" : 140343 }
// { "class" : "L", "total" : 23123 }
// { "class" : "P", "total" : 5683 }
// { "class" : "G", "total" : 17499 }



//-------------------------------------
//  1.2
//-------------------------------------
db.airlines.aggregate(
	[
		{
			$match: {
				"originCountry" : { $eq : "United States"},
				"destCountry" : { $ne : "United States"}
			}
		},
		{
			$project: {
			    _id: 0,
			    destCity: 1,
			    passengers: 1
			}
		},
		{
			$group: {
				_id: { destCity: "$destCity"},
				avgPassengers: {$avg: "$passengers"}
			}
		},
		{
			$sort: {
				"avgPassengers": -1
			}
		},
		{
			$limit: 3
		},
		{
			$project: {
				_id : 0,
				avgPassengers: "$avgPassengers",
				city: "$_id.destCity"
			}
		}
	]
);
// Result:
// { 
//     "avgPassengers" : 8052.380952380952, 
//     "city" : "Abu Dhabi, United Arab Emirates"
// }
// { 
//     "avgPassengers" : 7176.596638655462, 
//     "city" : "Dubai, United Arab Emirates"
// }
// { 
//     "avgPassengers" : 7103.333333333333, 
//     "city" : "Guangzhou, China"
// }


//-------------------------------------
//  1.3
//-------------------------------------
db.airlines.aggregate(
	[
		{
			$match: {
				"destCountry" : { $eq : "Latvia"}
			}
		},
		{
			$group: {
				_id: { destCountry: "$destCountry"},
				carriers: {$addToSet: "$carrier"}
			}
		},
		{
			$project: {
			    _id : "$_id.destCountry",
			    carriers: "$carriers"
			}
		},

	]
);
// Result
// { 
//     "_id" : "Latvia", 
//     "carriers" : [
//         "Uzbekistan Airways", 
//         "Blue Jet SP Z o o", 
//         "JetClub AG"
//     ]
// }
