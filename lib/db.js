var config  = require('config'),
    util    = require('wrms-dash-util'),
    pg      = require('pg');

'use strict';

function DB(driver){
    let self = this;

    this.driver = driver || pg;
    this.client = null;
    this.config = config.get('db');

    this.on_connect = null;
    this.connected = false;

    this.on_error = null;
    this.query_reject_ref = null;

    process.nextTick(function(){ reconnect(self) });
}

function reconnect(o, done){
    function handle_error(err){
        util.log(__filename, "ERROR: Couldn't connect to database - " + (err.stack || err));
        setTimeout(function(){ reconnect(o) }, 5*1000);
    }
    if (o.client){
        try{
            o.client.end();
        }catch(ex){ /* don't care */ }
    }

    o.client = new o.driver.Client(o.config);
    o.connected = false;

    o.client.on('error', function(err){
        o.connected = false;
        util.log(__filename, 'ERROR: ' + (err.stack || err));
        if (o.on_error){
            o.on_error(err);
        }
        if (o.query_reject_ref){
            o.query_reject_ref(err);
            o.query_reject_ref = null;
        }
        reconnect(o);
    });

    o.client.connect(function(err){
        if (err){
            handle_error(err);
        }else{
            o.connected = true;
            util.log(__filename, "Connected to database");
            if (o.on_connect){
                o.on_connect();
            }
        }
        done && done(err);
    });
}

DB.prototype.query = function(){
    if (!this.client || !this.connected){
        return new Promise((_, reject) => { reject(new Error('DB.query aborted, not connected yet')); });
    }
    let start = new Date(),
        args = Array.prototype.slice.call(arguments, 0),
        query_name = args.shift(),
        ctx = args.pop(),
        label = ctx ? `${ctx.org}/${ctx.sys}/${ctx.period}/${query_name}` : query_name;

    // Note: we usually want debug on a per-query basis, so using the
    // API_DEBUG env isn't good enough. To turn on debugging, throw
    // the text "-debug" on the end of the query name.
    const DEBUG = query_name.indexOf('debug') > -1;

    util.log_debug(__filename, query_name + ': ' + args[0], DEBUG);

    return new Promise((resolve, reject) => {
        args.push(function(err, data){
            this.query_reject_ref = null;
            let end = new Date();
            data = data || {rows: []};
            util.log(__filename, `${label}: ${data.rows.length} rows, rtt ${end.getTime() - start.getTime()}ms`);
            if (err){
                reject(err);
            }else{
                util.log_debug(__filename, `${label}: ${JSON.stringify(data.rows, null, 2)}`, DEBUG);
                let j = JSON.stringify(data, null, 2);
                resolve(JSON.parse(j));
            }
        });
        try{
            this.client.query.apply(this.client, args);
            this.query_reject_ref = reject;
        }catch(ex){
            util.log(__filename, 'ERROR: ' + ex);
            reject(ex);
        }
    });
}

let instance = undefined;

module.exports = {
    type: DB,
    create: function(){ instance = new DB(); return instance; },
    get: function(){
        if (!instance){
            instance = new DB();
        }
        return instance;
    },
    __test_override: function(i){ instance = i; }
}

