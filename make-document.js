/**
 * Created by Jonathan on 10/3/2015.
 */
 
var settings = require('./conf/settings.js');
var mysql = require('mysql');
var q = require('q');
var _ = require('lodash');
var elasticsearch = require('elasticsearch');
var util = require('util');

var pool = mysql.createPool(settings.mysqlPoolSettings);





function getObsValue(obs) {

    switch(obs) {
        case obs.value_coded :
            return obs.value_coded;
            break;
        case obs.value_numeric :
            return obs.value_numberic;
            break;
        case obs.value_boolean :
            return obs.value_boolean;
            break;
        case obs.value_datetime :
            return obs.value_datetime;
            break;
        case obs.value_drug :
            return obs.value_drug;
            break;
        case obs.value_text :
            return obs.value_text;
            break;
        default:
            return "";

    }
}

function makeEncounters(obsSet) {
    var encounters = {};
    var value;
    var obs;
    for(var i in obsSet) {
        obs = obsSet[i];
        value = getObsValue(obs);
        if(obs.encounter_id) {
            if(obs.encounter_id in encounters) {
                encounter = encounters[obs.encounter_id];
            }
            else {
                encounter = {encounter_id:obs.encounter_id,obsSet:{}};
                encounters[obs.encounter_id] = encounter;
            }

            if(obs.concept_id in encounter.obsSet) {
                encounter.obsSet[obs.concept_id].push(obs)
            }
            else encounter.obsSet[obs.concept_id] = [obs];
        }


    }

    return encounters;
}


//encounterIds: array of unique encounterIds
function getEncounters(encounterIds) {

    var defer = q.defer();
    pool.getConnection(function(err,connection) {
        if(err) {
            result.errorMessage = "Database Connection Error";
            result.error = err;
            console.log('Database Connection Error');
            callback(result);
            return defer.reject(err);
        }
        ids = encounterIds.join();

	/*
        var query = "select * from amrs.encounter t1"
	query += " join amrs.encounter_type t2 on t1.encounter_type = t2.encounter_type_id"
	query += " where t1.voided=0 and encounter_id in (" + ids + ")";
	*/

	var query = "select * from amrs.encounter t1 where t1.voided=0 and encounter_id in (" + ids + ")";
	console.log(query);
        connection.query(query,
            function(err,rows,fields) {
                return defer.resolve(rows);
            }
        );
        connection.release();
    });
    return defer.promise;
}

//getEncounters([600]).then(function(d) { console.log(d);});


function getEncounterIds(obsSet) {
    var encounterIds = [];
    for(var i=0 in obsSet) {
	var obs = obsSet[i];
        if(obs.encounter_id) encounterIds.push(obs["encounter_id"])
    }
    return _.uniq(encounterIds);
}



function getObs(personId) {

    var defer = q.defer();
    pool.getConnection(function(err,connection) {
        if(err) {
            result.errorMessage = "Database Connection Error";
            result.error = err;
            console.log('Database Connection Error');
            callback(result);
            return defer.reject(err);
        }
        var query = "select * from amrs.obs where voided=0 and person_id=" + personId;
	
        connection.query(query,
			 function(err,rows,fields) {			     
			     return defer.resolve(rows);
			 }
			);
        connection.release();
    });
    return defer.promise;
}




function getPatientData(personId) {
    var defer = q.defer();
    var data = {};
    getObs(personId)
        .then(function(obsRows) {
            data["obsSet"] = obsRows;
	    var ids = getEncounterIds(obsRows);
            getEncounters(ids).then(function(encounterRows) {				
                data["encounters"] = encounterRows;
                defer.resolve(data);
            });
        });
    return defer.promise;
}


function getPatients() {
    pool.getConnection(function(err,connection) {
        if(err) {
            result.errorMessage = "Database Connection Error";
            result.error = err;
            console.log('Database Connection Error');
            callback(result);
            return defer.reject(err);
        }
        //var query = "select count(*) as total from amrs.obs where voided=0";
	var query = "select count(*) as total from etl.person";

        connection.query(query,
			 function(err,rows,fields) {
			     var total = rows[0].total;
			     var maxRows = 1000;
			     var iterations = Math.floor(total/maxRows);
			     for(var i=0;i<= iterations;i++) {
				 //query = "select person_id from amrs.person where voided=0  limit " + maxRows + "," + (i*maxRows);
				 query = "select person_id from etl.person limit " + maxRows + "," + (i*maxRows);
				 console.log(query);
				 connection.query(query,
						  function(err,rows,fields) {
						      
						      for(var i=0 in rows) {
							  person_id = rows[i]["person_id"];							  
							  addToElastic(person_id);
						      }
						  });
			     }
			 }
			);
        connection.release();
    });
}


function getPatients2() {
    pool.getConnection(function(err,connection) {
        if(err) {
            result.errorMessage = "Database Connection Error";
            result.error = err;
            console.log('Database Connection Error');
            callback(result);
            return defer.reject(err);
        }
        //var query = "select count(*) as total from amrs.obs where voided=0";
	var query = "select person_id from etl.person";

        connection.query(query,
			 function(err,rows,fields) {			     			     
			     for(var i in rows) {
				 console.log(i);
				 //query = "select person_id from amrs.person where voided=0  limit " + maxRows + "," + (i*maxRows);
				 person_id = rows[i]["person_id"];							  
				 
				 //addToElastic(person_id);
			     }
			     addToElastic(rows[0].person_id);
			 });
        connection.release();
    });
}



/*
function test(lastUpdate) {

    //Get any obs that have been voided since last update
    var query = "select encounter_id from amrs.obs where t2.voided=1 and t2.date_voided >= " + lastUpdate + " and date_created<= " + lastUpdate;
    
    //Get encounter UUIds of voided obs
    

    //add encounter_ids to table of encounters to get from amrs
    
    //get encounter_id of obs with voided=0 and date_created >= lastUpdate;
    //get encounter uuids for encounter_ids
    

    //delete in elastic any encounter with encounterUUid above

    //build encounters for all above
    //add to elastic
}
*/
/*
function getObsByDate(startDate) {
    var defer = q.defer();
    pool.getConnection(function(err,connection) {
        if(err) {
            result.errorMessage = "Database Connection Error";
            result.error = err;
            console.log('Database Connection Error');
            callback(result);
            return defer.reject(err);
        }
        var query = "select count(*) as total from amrs.obs where voided=0 and date_created >= " + startDate;
	
        connection.query(query,
			 function(err,rows,fields) {			     
			     var total = rows[0].total;
			     var maxRows = 10000;
			     var iterations = Math.floor(total/max);
			     for(var i=0;i<= iterations;i++) {
				 query = "select * from amrs.obs where voided=0 and date_created >= " + startDate + " limit " + maxRows + "," + (i*maxRows);
				 connection.query(query,

			 }
			);
        connection.release();
    });
    return defer.promise;
}
*/

function makeEncounterDocuments(personId) {
    var defer = q.defer();
    var encounters = {};
    var encounterRow, encounter, obs,obsSet;
    getPatientData(personId).then(function(data) {

	for(var i=0 in data.encounters) {
	    encounterRow = data.encounters[i];
	    encounter = {
		"encounterDatetime": encounterRow["encounter_datetime"],
		"encounterType": encounterRow["encounter_type"],
		"encounterDatetime": encounterRow["encounter_datetime"],
		"encounterId": encounterRow["encounter_id"],
		"encounterUuid": encounterRow["uuid"],
		"voided":encounterRow.voided,
		"dateVoided":encounterRow.date_voided,
		"dateCreated":encounterRow.data_created,
		
		"obsSet":[]
	    }
	    encounters[encounterRow.encounter_id] = encounter;
	    
	}

	for(var i=0 in data.obsSet) {
	    obsRow = data.obsSet[i];
	    obs = {
		"obsId":obsRow.obs_id,
		"obsUuid":obsRow.uuid,
		"obsDatetime":obsRow.obs_datetime,
		"conceptId":obsRow.concept_id,
		"voided":obsRow.voided,
		"dateCreated":obsRow.date_created,		
	    }
	    if(obsRow.voided) obs["dateVoided"] = obsRow.date_voided;

	    if(obsRow.value_coded) obs["valueCoded"] = obsRow.value_coded;
	    else if(obsRow.value_boolean) obs["valueBoolean"] = obsRow.value_boolean;
	    else if(obsRow.value_datetime) obs["valueDatetime"] = obsRow.value_datetime;
	    else if(obsRow.value_numeric) obs["valueNumeric"] = obsRow.value_numeric;
	    else if(obsRow.value_text) obs["valueText"] = obsRow.value_text;
	    
	    if(obsRow.encounter_id) {
		encounter = encounters[obsRow.encounter_id];
		encounter.obsSet.push(obs);
	    }
	}
	defer.resolve(encounters);
    });
    return defer.promise;
}



function addToElastic(patientId) {

    var body = [];
    var encounter;
    makeEncounterDocuments(patientId).then(function(patientEncounters) {

	for(var i in patientEncounters) {
	    encounter = patientEncounters[i];
	    body.push({index:{"_index":"amrs","_type":"encounter","_id":encounter.encounterId}});
	    body.push(encounter);	    
	}
	
	var client = new elasticsearch.Client({
	    host: '104.236.65.80:9200',
	    log: 'trace'
	});
	var payload = {
	    body: body
	};
	
	client.bulk(payload,function(error){});
    });
}


var encounterMapping =
{

        "encounter": {
            "properties": {
                encounterId: {"type": "integer"},
                encounterUuid: {"type": "string"},		
                encounterDatetime: {"type": "date"},
		encounterTypeId: {"type":"integer"},
                encounterTypeUuid: {"type": "string"},
                encounterTypeName: {"type": "string"},
                formUuid: {"type": "string"},
                formName: {"type": "string"},
                locationUuid: {"type": "string"},
                locationName: {"type": "string"},
		locationId: {"type":"integer"},
                providers: {"type": "string"},
                dateCreated: {"type": "date"},
                voided: {"type": "boolean"},
                dateVoided: {"type": "date"},
                obsSet: {
                    "type": "nested",
                    "properties": {
			obsId:{"type":"integer"},
			obsUuid:{"type":"string"},
                        obsGroupId: {"type":"integer"},
                        dateCreated: {"type": "date"},
                        obsDatetime: {"type": "date"},
                        conceptId: {"type": "integer"},
                        conceptUuid: {"type": "string"},
                        valueCoded: {"type": "integer"},
                        valueBoolean: {"type": "boolean"},
                        valueNumeric: {"type": "float"},
                        valueDatetime: {"type": "date"},
                        valueText: {"type": "string"},
                        valueDrug: {"type": "integer"},
                        valueGroupId: {"type": "integer"},
                        formUuid: {"type": "string"},
                        formName: {"type": "string"},
                        locationUuid: {"type": "string"},
                        locationName: {"type": "string"},
                        voided: {"type": "boolean"},
                        dateVoided: {"type": "date"}
                    }
                }
            }
        }

}


function addEncounterMapping() {
    var client = new elasticsearch.Client({
	host: '104.236.65.80:9200',
	log: 'trace'
    });
    
    client.indices.putMapping(
	{
	    index:"amrs",
	    type:"encounter",
	    body:encounterMapping
	}
    );
}
//addEncounterMapping();    
getPatients2();    




//getPatientData(600).then(function(data) {console.log(data);});

//145991

/*
addToElastic(600);
addToElastic(145991);
addToElastic(657);
addToElastic(715);
addToElastic(760);
addToElastic(786);
addToElastic(798);
addToElastic(845);
addToElastic(859);
addToElastic(964);
*/
//console.log(e);

