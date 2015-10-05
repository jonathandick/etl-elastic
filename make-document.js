/**
 * Created by Jonathan on 10/3/2015.
 */

var settings = require('./conf/settings.js');
var mysql = require('mysql');
var q = require('q');
var _ = require('lodash');

var pool = mysql.createPool(settings.mysqlPoolSettings);

var labMapping

var mapping =
{
    "mappings": {
        "encounter": {
            "properties": {
                encounterDatetime: {"type": "date"},
                encounterTypeUuid: {"type": "string"},
                encounterTypeName: {"type": "string"},
                formUuid: {"type": "string"},
                formName: {"type": "string"},
                locationUuid: {"type": "string"},
                locationName: {"type": "string"},
                encounterId: {"type": "integer"},
                encounterUuid: {"type": "string"},
                providers: {"type": "string"},
                dateCreated: {"type": "date"},
                voided: {"type": "boolean"},
                dateVoided: {"type": "date"},
                obsSet: {
                    "type": "nested",
                    "properties": {
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
}



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
        ids = "";
        _.forEach(encounterIds,function(id) {
            ids += id + ",";
        });


        var query = "select * from amrs.encounter where voided=0 and encounter_id in (" + ids + ")";

        connection.query(query,
            function(err,rows,fields) {
                return defer.resolve(rows);
            }
        );
        connection.release();
    });
    return defer.promise;
}


function getEncounterIds(obsSet) {
    encounterIds = [];
    _.foreach(obsSet,function(obs) {
        encounterIds.push(obs["encounter_id"])
    });
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
    getObs(person_id)
        .then(function(obsRows) {
            data["obsSet"] = obsRows;
            getEncounterIds(obsSet).then(function(encounterRows) {
                data["encounters"] = encounterRows;
                defer.resolve(data);
            });
        });
    return defer.promise;
}

/*
function getHivStartDate(o,prevDocument) {
    var result =
        (obs[120].value === 100)
        || (obs[130]["group"][100]["value"] === 100);

    return result;
    var obsSet =
    {
        120: {
            concept: "",
            valueCoded | valueNumeric | valueText | : "",
            obsSet: {
                300:{concept,value,group}
            }
            }
        }
    }
}
*/


function makeDocuments(encounters) {
    var documents = [];
    var prevDocument, document, encounter;
    for(var i in encounters) {
        encounter = encounters[i];
        prevDocument = document;
        document = makeDocument(encounter,prevDocument);
        documents.push(document);
    }
    return documents;
}


function makeDocument(encounter,prevDocument) {
    var indicators = [{"field":"hivStartDate","function":getHivStartDate}];
    var indicator,result;
    var document;
    for(var i in indicators) {
        indicator = indicators[i];
        //result = indicator["function"](encounter.obsSet,prevDocument);
        //document[indicator.field] = result;
        result = getHivStartDate(encounter["obsSet"]);
    }
    console.log("hello make document again");

    console.log(document);
    return document;
}

//145991

getObs(600).then(function(obsSet) {
    console.log(obsSet);
    var encounters = makeEncounters(obsSet);
    var documents = makeDocuments(encounters);
    console.log('finished');
});




