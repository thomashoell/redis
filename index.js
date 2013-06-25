/**
 * Created with JetBrains WebStorm.
 * User: thomas
 * Date: 17.05.13
 * Time: 11:36
 * To change this template use File | Settings | File Templates.
 */

    /*
     {    "id": 1,    "incoming_date": "2013-17-05 11:45",    "processing_date": "2013-17-05 11:46",    "end_date": "2013-17-05 11:47",    "filename": "my_pdf.pdf",    "status": "queued",    "errors": null,    "messages": null,    "deleted": true,    "meta": null}
    */

var redis = require("redis")
var client = redis.createClient(null, null, {})

function process (data, cb){

    var json_object;
    try{
        json_object = JSON.parse(data)
    }catch(err){
        json_object = null
        //console.log(err)
    }
    if (json_object){
        console.log('-------processing JSON data-------')
        console.log(json_object)
        console.log('---------------done---------------')
    }
    if (typeof(cb)=='function'){
        cb(null)
    }
}

function process_stack (cb){
    client.rpop('test', function (err,val){
        if (err){
            console.log(err)
            cb(err)
        }
        else{
            if (val){
                process(val)
                process_stack(cb)
            }
            else{
                cb(1)
            }
        }
    })
}

client.on("error", function (err) {
    if (String(err).search('ECONNREFUSED') > 0){
        console.log('connection refused')
        client.end()
    }
    else
        console.log('err code: '+err)
});

client.on('ready', function (status){

})

client.on('connect', function (status){
    process_stack(function(err){
        client.monitor(function (err, res) {
            console.log("Entering monitoring mode.");
        });
    })

})


client.on("monitor", function (time, args) {
    if (args[0] == 'rpush' && args[1] == 'test'){
        client.rpop(args[1], function (err,val){
            if (!err){
                process(val)
            }
        })
    }
})
