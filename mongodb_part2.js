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
				_id: { originState: "$originState", 
					   originCity: "$originCity" },
				totalPassengers: {$sum: "$passengers"}
			}
		},
		{
			$sort: {totalPassengers : -1}
		},
		{
			$group: {
				_id : { originState : "$_id.originState"},
				originCity : { $first : "$_id.originCity"},
				totalPassengers : { $first : "$totalPassengers"}
			}
		},
		{
			$sort: {
				"_id.originState" : 1
			}
		},
		{
			$limit: 5
		},
		{
			$project: {
			  	_id : 0,
				totalPassengers : "$totalPassengers",
				location : { state : "$_id.originState", city : "$originCity" }
			}
		}
	]
);
// Result:
// { "totalPassengers" : 760120, "location" : { "state" : "Alabama", "city" : "Birmingham, AL" } }
// { "totalPassengers" : 1472404, "location" : { "state" : "Alaska", "city" : "Anchorage, AK" } }
// { "totalPassengers" : 13152753, "location" : { "state" : "Arizona", "city" : "Phoenix, AZ" } }
// { "totalPassengers" : 571452, "location" : { "state" : "Arkansas", "city" : "Little Rock, AR" } }
// { "totalPassengers" : 23701556, "location" : { "state" : "California", "city" : "Los Angeles, CA" } }

//-------------------------------------
// Task For Enron Db
//-------------------------------------
db.enron.aggregate(
	[
		{
			$project: {
			  	Mail_id : "$_id",
				From : "$headers.From",
				To : "$headers.To"
			}
		},
		{
			$unwind: "$To"
		},
		{
			$group: {
				_id : "$Mail_id",
				From : { $first: "$From" },
				To : { $addToSet : "$To" }
			}
		},
		{
			$unwind: "$To"
		},
		{
			$group: {
			    _id: {
			            From: "$From",
			            To: "$To"
			    },
			    count: {$sum: 1}
			}
		},
		{
			$sort: {
				count : -1
			}
		},
		{
			$limit: 1
		},
		{
			$project: {
				_id : 0,
				From : "$_id.From",
				To : "$_id.To",
				count: "$count"
			}
		}
	]
);

// Result:
// { "From" : "susan.mara@enron.com", "To" : "jeff.dasovich@enron.com", "count" : 750 }
