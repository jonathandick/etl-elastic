var settings = require('./conf/settings.js');
var q = require('q');
var _ = require('lodash');
var elasticsearch = require('elasticsearch');


function createGetEncountersQuery(locationIds,startDate,endDate) {
    var getEncountersQuery = {
        "query": {
            "filtered": {
                "filter": {
                    "bool": {
                        "must": [
                            {
                                "terms": {
                                    "location_id": locationIds
                                }
                            },
                            {
                                "terms": {
                                    "encounter_type": [
                                        1,
                                        2,
                                        3,
                                        4
                                    ]
                                }
                            },
                            {
                                "range": {
                                    "obs_datetime": {
                                        "from": startDate,
                                        "to": endDate
                                    }
                                }
                            }
                        ]
                    }
                }
            }
        },
        "aggs": {
            "person_ids": {
                "terms": {
                    "field": "person_id",
                    "size": 10000
                },
                "aggs": {
                    "encounter_ids": {
                        "terms": {
                            "field": "encounter_id",
                            "order": {"max_obs_datetime": "desc"}
                        },
                        "aggs": {
                            "max_obs_datetime": {
                                "max": {"field": "obs_datetime"}
                            }
                        }
                    }
                }
            }
        }
    };
    return getEncountersQuery;
}

/*
locationIds: array of locationIds, e.g. [1,2,3,4]
startDate,endDate: string in YYYY-MM-DD format
encounterIds: array of encounterIds, e,g, [1,2,3,4].
 */
function createMOH731ReportQuery(locationIds, startDate,endDate,encounterIds) {
    var moh731ReportQuery =
    {
        "query": {
            "filtered": {
                "filter": {
                    "bool": {
                        "must": [
                            {
                                "terms": {
                                    "location_id": locationIds
                                }
                            },
                            {
                                "terms": {
                                    "encounter_type": [
                                        1,
                                        2,
                                        3,
                                        4
                                    ]
                                }
                            },
                            {
                                "range": {
                                    "obs_datetime": {
                                        "from": startDate,
                                        "to": endDate
                                    }
                                }
                            },
                            {
                                "terms": {
                                    "encounter_id": encounterIds
                                }
                            },

                        ]
                    }
                }
            }
        },
        "aggs": {
            "months": {
                "date_histogram": {
                    "field": "obs_datetime",
                    "interval": "month"
                },
                "aggs": {
                    "locations": {
                        "terms": {
                            "field": "location_id"
                        },
                        "aggs": {
                            "genders": {
                                "filters": {
                                    "filters": {
                                        "males": {
                                            "term": {
                                                "gender": "M"
                                            }
                                        },
                                        "females": {
                                            "term": {
                                                "gender": "F"
                                            }
                                        }
                                    }
                                },
                                "aggs": {
                                    "started_arvs": {
                                        "filter": {
                                            "bool": {
                                                "must": [
                                                    {
                                                        "term": {
                                                            "concept_id": 1255
                                                        }
                                                    },
                                                    {
                                                        "term": {
                                                            "value_coded": 1256
                                                        }
                                                    }
                                                ]
                                            }
                                        },
                                        "aggs": {
                                            "distinct_encounters": {
                                                "cardinality": {
                                                    "field": "encounter_id",
                                                    "precision_threshold": 100
                                                }
                                            },
                                            "distinct_patients": {
                                                "cardinality": {
                                                    "field": "person_id",
                                                    "precision_threshold": 10
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    };
    return moh731ReportQuery;
}


function search(query,callback) {
    var client = new elasticsearch.Client({
        host: 'https://etl.ampath.or.ke/elastic',
        log: 'trace'
    });

    var encounterIds =[];
    client.search({
        searchType:"count",
        index:"amrs",
        type:"obs",
        body:query
    }).then(function(resp){callback(resp);},function(err){
        console.log(err);
    });

}


function getMOH731(locationIds,startDate,endDate) {
    var encounterIds = [];
    var doReportQuery = function(resp) {
        console.log("# of buckets: ",resp.aggregations.person_ids.buckets.length);
        _.each(resp.aggregations.person_ids.buckets,function(bucket) {
            encounterIds.push(bucket.encounter_ids.buckets[0].key)
        });
        console.log("running MOH731 query...");
        var getMOH731ReportQuery = createMOH731ReportQuery(locationIds,startDate,endDate,encounterIds);
        search(getMOH731ReportQuery,function(response) {
            console.log(response);
        })
    };

    var getEncountersQuery = createGetEncountersQuery(locationIds,startDate,endDate);
    search(getEncountersQuery,doReportQuery);
}

getMOH731([13],"2006-09-01","2006-09-30");