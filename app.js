const fs = require('fs');
const { Client } = require('elasticsearch')
const MongoClient = require('mongodb').MongoClient;
var ca = [fs.readFileSync("rds-combined-ca-bundle.pem")];

const migrate = async () => {
    
    try {
        
        var setupStatus = false
        
        if (fs.existsSync('settings.json')) {
            var settings = JSON.parse(fs.readFileSync('settings.json'));
        }
        
        //TODO VALIDATE SETTINGS FILE
        let es_settings = settings.es
        let mongo_settings = settings.mongo
        // es.connectionUrl = `https://${es_settings.url}/${es_settings.index}`
        
        //ES Client Setup
        var es_client = new Client({
            hosts: [`https://${es_settings.username}:${es_settings.password}@${es_settings.url}`]
        })
        
        //Mongo Client Setup
        var mongodb_auth_string = mongo_settings.username || mongo_settings.password ? `${mongo_settings.username}:${mongo_settings.password}@` : ''
        var mongodb_url = `mongodb://${mongodb_auth_string}${mongo_settings.url}:${mongo_settings.port}`
        const mongodb_client = await MongoClient.connect(mongodb_url, { ssl: true, sslCA: ca })
        .catch(err => { console.log(err); });
        if (!mongodb_client) {
            return;
        }
        const mongo_db = mongodb_client.db(mongo_settings.db);
        const mongo_collection = mongo_db.collection(mongo_settings.collection)
        
        //ES GET COUNT OF RECORDS
        var es_records_count = (await es_client.count({
            index: `${es_settings.index}`
        })).count
        
        
        //ES GET RECORDS WITH PAGINATION,
        var es_records = []
        var mongo_insert_success_count = 0
        var mongo_insert_failure_count = 0
        let page_length = es_settings.page_length ? es_settings.page_length : 20
        let pages_count = parseInt(es_records_count/page_length)
        setupStatus = true
        console.log("Total number of records in ES: ",JSON.stringify(es_records_count))
        console.log("Number of pages: ",pages_count + 1)
        console.log("Maximum Number of records per page: ",page_length)
        for (var i = 0,j=0; j  <= pages_count; i=i+page_length,j++) {
            es_records = (await es_client.search({
                index: `${es_settings.index}`,
                body: {
                    from: i,
                    size: page_length
                }
            })).hits.hits
            
            es_records = es_records.map(rec => rec._source)
            
            await mongo_collection.insertMany(es_records)
            .then(results => {
                console.log(`Loop ${j} Successful, inserted ${es_records.length} records`)
                mongo_insert_success_count +=  es_records.length
            })
            .catch(error => {
                console.log(`Loop ${j} Failure`)
                console.error(error)
                mongo_insert_failure_count += es_records.length
            })
        }
        
    } catch (error) {
        console.log("Something Went Wrong: \n",error)
    }
    
    if(setupStatus){   
        console.log("***************************")
        console.log("Total: ", es_records_count)
        console.log("Success:", mongo_insert_success_count)
        console.log("failure:", mongo_insert_failure_count)
        console.log("***************************")
    }
    
    return true;
}

migrate();

