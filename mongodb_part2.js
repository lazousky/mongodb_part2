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
		}
	]
);
// Result:
// { 
//     "_id" : "Latvia", 
//     "carriers" : [
//         "Uzbekistan Airways", 
//         "Blue Jet SP Z o o", 
//         "JetClub AG"
//     ]
// }


//-------------------------------------
//  1.4
//-------------------------------------
db.airlines.aggregate(
	[
		{
			$match: {
				"originCountry" : "United States",
				"destCountry": { $in: [ "Greece", "Italy", "Spain"] } 	
			}
		},
		{
			$group: {
				_id : { carrier: "$carrier"},
				total : { $sum : "$passengers"}
			}
		},
		{
			$project: {
			    _id : "$_id.carrier",
			    total : "$total"
			}
		},
		{
			$sort: {
				total : -1
			}
		},
		{
			$limit: 10
		},		
		{
			$skip: 3
		}
	]
);
// Result:
// { "_id" : "Compagnia Aerea Italiana", "total" : 280256 }
// { "_id" : "United Air Lines Inc.", "total" : 229936 }
// { "_id" : "Emirates", "total" : 100903 }
// { "_id" : "Air Europa", "total" : 94968 }
// { "_id" : "Meridiana S.p.A", "total" : 20308 }
// { "_id" : "Norwegian Air Shuttle ASA", "total" : 13344 }
// { "_id" : "VistaJet Limited", "total" : 183 }


//-------------------------------------
//  1.5
//-------------------------------------
db.airlines.aggregate(
	[
		{
			$match: {
				"originCountry" : "United States"
			}
		},
		{
			$group: {
				_id: { 
				  originState: "$originState", 
				  originCity: "$originCity" },
				 totalPassengers: {$sum: "$passengers"}
			}
		},
		{
			$sort: {
				totalPassengers : -1
			}
		},
		{
			$limit: 5
		},
		{
			$sort: {
				"_id.originState" : 1
			}
		},
		{
			$project: {
			    _id : 0,
			    totalPassengers : "$totalPassengers",
			    location : { state : "$_id.originState", city : "$_id.originCity" }
			}
		},

	]
);
// Result:
// { "totalPassengers" : 23701556, "location" : { "state" : "California", "city" : "Los Angeles, CA" } }
// { "totalPassengers" : 29416565, "location" : { "state" : "Georgia", "city" : "Atlanta, GA" } }
// { "totalPassengers" : 28035755, "location" : { "state" : "Illinois", "city" : "Chicago, IL" } }
// { "totalPassengers" : 25266639, "location" : { "state" : "New York", "city" : "New York, NY" } }
// { "totalPassengers" : 18408792, "location" : { "state" : "Texas", "city" : "Dallas/Fort Worth, TX" } }