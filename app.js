var app = require('express')();
var http = require('http').Server(app);
const uuidv4 = require('uuid/v4');
var bodyParser = require('body-parser')
app.use(bodyParser.urlencoded({
    extended: true
}));
app.use(bodyParser.json());
const { Pool, Client } = require('pg');

var Status = Object.freeze({ 0:"DRAFT", 1:"ACTIVE", 2:"CLOSED"})

const pool = new Pool({
    user: 'postgres',
    host: 'localhost',
    database: 'postgres',
    password: '',
    port: 5432,
})

const client = new Client({
    user: 'postgres',
    host: 'localhost',
    database: 'postgres',
    password: '',
    port: 5432,
})

const queryCheckIfChannelTableExists = {
    text: "SELECT to_regclass('public.channel')"
}

const queryCheckIfMessageTableExists ={
    text: "SELECT to_regclass('public.message')"
}

const queryCreateChannelTable = {
    text: 'CREATE TABLE channel(channel_name VARCHAR PRIMARY KEY,'+
                                'status VARCHAR NOT NULL,'+
                                'date_of_creation TIMESTAMP NOT NULL,'+
                                'date_of_closing TIMESTAMP)'
}

const queryCreateMessageTable = {
    text: 'CREATE TABLE message(message_id uuid PRIMARY KEY,'+
                                'account_id VARCHAR NOT NULL,'+
                                'data VARCHAR NOT NULL,'+
                                'date_of_creation TIMESTAMP NOT NULL,' +
                                'channel_name VARCHAR REFERENCES channel(channel_name))'
}

function shouldAbort(err, client, done){
    if (err) {
        console.error('Error in transaction!'/*, err.stack*/);
        client.query('ROLLBACK', (err) => {
            if (err) {
                console.error('Error rolling back client!'/*, err.stack*/);
            }
            console.log("Rolling back...");
            done()
            console.log("Done.")
        })
    }
    return !!err 
}

pool.connect((err, client, done) => {
    if (shouldAbort(err, client, done)) return
    client.query('BEGIN', (err) => {
        if (shouldAbort(err, client, done)) return
        client.query(queryCheckIfChannelTableExists, (err, res) => {
            if (shouldAbort(err, client, done)) return
            if(res.rows[0].to_regclass == null){
                console.log("Channel table doesn't exist. Creating...");
                client.query(queryCreateChannelTable, (err, res) => {
                    if (shouldAbort(err, client, done)) return
                    client.query(queryCheckIfMessageTableExists, (err, res) => {
                        if (shouldAbort(err, client, done)) return
                        if(res.rows[0].to_regclass == null){
                            console.log("Message table doesn't exist. Creating...");
                            client.query(queryCreateMessageTable, (err,res) => {
                                if (shouldAbort(err, client, done)) return
                                client.query('COMMIT', (err) => {
                                    if (err) {
                                        console.error('Error committing transaction!', err.stack);
                                    }
                                    done()
                                })
                            })
                        }
                        else{
                            console.log("Message table already exists!");
                            client.query('COMMIT', (err) => {
                                if (err) {
                                    console.error('Error committing transaction!', err.stack);
                                }
                                done()
                            })
                        }
                    })
                    
                })
            }
            else{
                console.log("Channel table already exists!");
                client.query(queryCheckIfMessageTableExists, (err, res) => {
                    if (shouldAbort(err, client, done)) return
                    if(res.rows[0].to_regclass == null){
                        console.log("Message table doesn't exist. Creating...");
                        client.query(queryCreateMessageTable, (err,res) => {
                            if (shouldAbort(err, client, done)) return
                            client.query('COMMIT', (err) => {
                                if (err) {
                                    console.error('Error committing transaction!', err.stack);
                                }
                                done()
                            })
                        })
                    }
                    else{
                        console.log("Message table already exists!");
                        done()
                    }
                })
            }
        })
    })
})

app.get('/channel', function(req, res){
    var response = res;
    const queryGetChannels = {
        text: "SELECT * FROM channel"
    }
    pool.connect((err, client, done) => {
        if (shouldAbort(err, client, done)) return
        client.query('BEGIN', (err) => {
            if (shouldAbort(err, client, done)) return
            client.query(queryGetChannels, (err, res) => {
                if (shouldAbort(err, client, done)){
                    response.sendStatus(404)
                    return
                }
                if(res.rowCount != 0){
                    response.send(res.rows);
                }
                else response.sendStatus(404);
            })
        })
    })
});

app.get('/channel/by-name/:channel_name', function(req, res){
    var response = res;
    const channelName = req.params.channel_name;
    const queryGetChannelByName = {
        text: "SELECT * FROM channel WHERE channel_name = $1",
        values: [channelName]
    }
    pool.connect((err, client, done) => {
        if (shouldAbort(err, client, done)) return
        client.query('BEGIN', (err) => {
            if (shouldAbort(err, client, done)) return
            client.query(queryGetChannelByName, (err, res) => {
                if (shouldAbort(err, client, done)){
                    response.sendStatus(404)
                    return
                }
                if(res.rowCount == 1){
                    response.send(res.rows[0]);
                }
                else response.sendStatus(404);
            })
        })
    })
});

app.get('/channel/by-name/:channel_name/messages', function(req, res){
    var response = res;
    var minutes = 0;
    if(req.query !== undefined) {
        minutes = req.query.minutes;
    }
    const channelName = req.params.channel_name;
    const queryGetChannelMessages = {
        text: "SELECT * FROM message WHERE channel_name = $1",
        values: [channelName]
    }
    pool.connect((err, client, done) => {
        if (shouldAbort(err, client, done)) return
        client.query('BEGIN', (err) => {
            if (shouldAbort(err, client, done)) return
            client.query(queryGetChannelMessages, (err, res) => {
                if (shouldAbort(err, client, done)){
                    response.sendStatus(500)
                    return
                }
                if(minutes !== 0){
                    var validMessages = [];
                    res.rows.forEach(function(i){
                        if(Date.parse(i.date_of_creation) >= (Date.now() - (minutes*60000))){
                            validMessages.push(i);
                        }
                    });
                    response.send(validMessages);
                }
                else response.send(res.rows);
            })
        })
    })
})

app.post('/channel', function(req, res){
    var response = res;
    const channelName = req.body.name;
    const channelStatus = req.body.status;
    const queryCreateChannel = {
        text: 'INSERT INTO channel(channel_name, status, date_of_creation, date_of_closing) VALUES($1, $2, $3, $4)',
        values: [channelName, Status[channelStatus], new Date(Date.now()), null]
    }

    pool.connect((err, client, done) => {
        if (shouldAbort(err, client, done)) return
        client.query('BEGIN', (err) => {
            if (shouldAbort(err, client, done)) return
            client.query(queryCreateChannel, (err, res) => {
                if (shouldAbort(err, client, done)){
                    console.log("Channel already exists!");
                    response.sendStatus(409);
                    return 
                }
                console.log("Channel doesn't exist. Creating...");
                client.query('COMMIT', (err) => {
                    if (err) {
                        console.error('Error committing transaction', err.stack);
                    }
                    done()
                    response.sendStatus(201);
                })
            })
        })
    })
});

app.post('/channel/:channel_name/message', function(req, res){
    var response = res;
    const channelName = req.params.channel_name;
    const accountId = req.body.accountId;
    const authorization = req.body.authorization;
    const data = req.body.data;
    var request=require('request');
    var options = {
        url: 'https://dev.onair-backend.moon42.com/api/business-layer/v1/chat/account/' + accountId + '/channel/' + channelName,
        headers: {
            'authorization': authorization
        }
    };

    const queryNewMessage = {
        text : "INSERT INTO message(message_id, account_id, data, date_of_creation, channel_name) VALUES($1, $2, $3, $4, $5)",
        values : [uuidv4(), accountId, data, new Date(Date.now()), channelName]
    }

    request.get(options, function(error, res, body){
        if(!error && res.statusCode == 200){
            var responseJson = JSON.parse(body);
            if(responseJson.canWrite){
                if(sockets.get(channelName) != undefined) sockets.get(channelName).emit('sendMessage', data);
                pool.connect((err, client, done) => {
                    if (shouldAbort(err, client, done)) return
                    client.query('BEGIN', (err) => {
                        if (shouldAbort(err, client, done)) return
                        client.query(queryNewMessage, (err, res) => {
                            if (shouldAbort(err, client, done)) return
                            console.log("Message saved.");
                            client.query('COMMIT', (err) => {
                                if (err) {
                                    console.error('Error committing transaction', err.stack);
                                }
                                response.sendStatus(201);
                                done()
                            })
                        })
                    })
                })
            }
        }
        else{
            response.sendStatus(res.statusCode);
            console.log("NOT AUTHORIZED");
        }
    })
})

app.patch('/channel/by-name/:channel_name', function(req, res){
    var response = res;
    const channelName = req.params.channel_name;
    const channelStatus = req.body.status;
    const querySetChannelStatus = {
        text: "UPDATE channel SET status = $1 WHERE channel_name = $2",
        values: [Status[channelStatus], channelName]
    }
    pool.connect((err, client, done) => {
        if (shouldAbort(err, client, done)) return
        client.query('BEGIN', (err) => {
            if (shouldAbort(err, client, done)) return
            client.query(querySetChannelStatus, (err, res) => {
                if (shouldAbort(err, client, done)){
                    response.sendStatus(404)
                    return
                }
                client.query('COMMIT', (err) => {
                    if (err) {
                        console.error('Error committing transaction', err.stack);
                    }
                    done()
                    console.log("Channel status updated.");
                    response.sendStatus(200);
                })
            })
        })
    })
})

var sockets = new Map();

var sessionData = new Map();

app.get('/channel/:channel_name', function(req, res){
    res.sendFile(__dirname + '/public/index.html');
    
    const channelName = req.params.channel_name;

    const queryCreateChannel = {
        text: 'INSERT INTO channel(channel_name, status, date_of_creation, date_of_closing) VALUES($1, $2, $3, $4)',
        values: [channelName, Status[1], new Date(Date.now()), null]
    }

    pool.connect((err, client, done) => {
        if (shouldAbort(err, client, done)) return
        client.query('BEGIN', (err) => {
            if (shouldAbort(err, client, done)) return
                client.query(queryCreateChannel, (err, res) => {
                    if (shouldAbort(err, client, done)){
                        console.log("Channel already exists!");
                        return
                    }
                    console.log("Channel doesn't exist. Creating...");
                    client.query('COMMIT', (err) => {
                        if (err) {
                            console.error('Error committing transaction', err.stack);
                        }
                        done()
                    })
                })
                
        })
    })
    if(sockets.get(channelName) === undefined){
        console.log("There is no socket for the channel! Creating...");
        const io = require('socket.io')(http)
        io.on('connection', function(session){
            console.log('User connected. Session:' + session.id);
            session.on('authorization', function(authorizationJson){
                var request = require('request');
                var options = {
                    url: 'https://dev.onair-backend.moon42.com/api/business-layer/v1/chat/account/' + authorizationJson.accountId + '/channel/' + channelName,
                    headers: {
                        'authorization': authorizationJson.authorization
                    }
                };
                request.get(options, function(error, response, body){
                    if(!error && response.statusCode == 200){
                        const responseJson = JSON.parse(body);
                        sessionData.set(session.id, responseJson);
                        if(sessionData.get(session.id).canRead){
                            var options = {
                                url: 'http://localhost:8080/channel/by-name/' + channelName + '/messages/?minutes=10'
                            }
                            request.get(options, function(error, response, body){
                                if(!error && response.statusCode == 200){
                                    const responseJson = JSON.parse(body);
                                    responseJson.forEach(function(i){
                                        session.send(i.data)
                                    });
                                }
                                else{
                                    console.log(response.statusCode);
                                }
                            })
                        }
                    }
                    else{
                        console.log("NOT AUTHORIZED");
                    }
                })
            })
            session.on('newMessage', function(data){
                if(sessionData.get(session.id) !== undefined && sessionData.get(session.id).canWrite){
                    io.emit('sendMessage', data)
                    const queryNewMessage = {
                        text : "INSERT INTO message(message_id, account_id, data, date_of_creation, channel_name) VALUES($1, $2, $3, $4, $5)",
                        values : [uuidv4(), sessionData.get(session.id).accountId, data, new Date(Date.now()), channelName]
                    }
                    pool.connect((err, client, done) => {
                        if (shouldAbort(err, client, done)) return
                        client.query('BEGIN', (err) => {
                            if (shouldAbort(err, client, done)) return
                            client.query(queryNewMessage, (err, res) => {
                                if (shouldAbort(err, client, done)) return
                                console.log("Message saved.");
                                client.query('COMMIT', (err) => {
                                    if (err) {
                                        console.error('Error committing transaction', err.stack);
                                    }
                                    done()
                                })
                            })
                        })
                    })
                }
                else{
                    console.log("NOT AUTHORIZED");
                }
            })
            session.on('disconnect', function(){
                console.log('User disconnected.');
                sessionData.delete(session.id);
            })
            sockets.set(channelName, io);
        })
    }
    else{
        console.log("Socket already exists!");
    }
});

http.listen(8080, function(){
    console.log('Listening on localhost:8080');
});