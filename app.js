const app = require('express')();
const http = require('http').Server(app);
const uuidv4 = require('uuid/v4');
const bodyParser = require('body-parser')
app.use(bodyParser.urlencoded({
    extended: true
}));
app.use(bodyParser.json());
const { Pool, Client } = require('pg');

const Status = Object.freeze({ 0:"DRAFT", 1:"ACTIVE", 2:"CLOSED"})

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
        console.error('Error in transaction: ' + err.message);
        client.query('ROLLBACK', (err) => {
            if (err) console.error('Error rolling back client!');
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
        client.query(queryCreateChannelTable, (err, res) => {
            if (err) {
                console.error('Error in transaction: ' + err.message);
                client.query('ROLLBACK', (err) => {
                    if (err) console.error('Error rolling back client!');
                    console.log("Rolling back...");
                })
                client.query(queryCreateMessageTable, (err, res) => {
                    if (shouldAbort(err, client, done)) return
                    console.log("Message table doesn't exist. Creating...");
                    client.query('COMMIT', (err) => {
                        if (err) console.error('Error committing transaction: ', err.message);
                        done()
                    })
                })
            }
            else{
                console.log("Channel table doesn't exist. Creating...");
                client.query(queryCreateMessageTable, (err, res) => {
                    if (shouldAbort(err, client, done)) return
                    console.log("Message table doesn't exist. Creating...");
                    client.query('COMMIT', (err) => {
                        if (err) console.error('Error committing transaction: ', err.message);
                        done()
                    })
                })
            }
        })
    })
})

app.get('/channel', function(request, response){
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
                if(res.rowCount != 0)response.send(res.rows);
                else response.sendStatus(404);
            })
        })
    })
});

app.get('/channel/by-name/:channelName', function(request, response){
    const channelName = request.params.channelName;
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
                if(res.rowCount == 1) response.send(res.rows[0]);
                else response.sendStatus(404);
            })
        })
    })
});

app.get('/channel/by-name/:channelName/messages', function(request, response){
    let minutes = 0;
    if(request.query.minutes !== undefined) {
        minutes = request.query.minutes;
    }
    const channelName = request.params.channelName;
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
                    let validMessages = [];
                    res.rows.forEach(function(i){
                        if(new Date(i.date_of_creation) >= (new Date().getTime() - (minutes*60000))) validMessages.push(i);
                    });
                    response.send(validMessages);
                }
                else response.send(res.rows);
            })
        })
    })
})

app.post('/channel', function(request, response){
    const channelName = request.body.name;
    const channelStatus = request.body.status;
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
                    response.sendStatus(409);
                    return 
                }
                console.log("Channel doesn't exist. Creating...");
                client.query('COMMIT', (err) => {
                    if (err) {
                        console.error('Error committing transaction: ', err.message);
                    }
                    done()
                    response.sendStatus(201);
                })
            })
        })
    })
});

app.post('/channel/:channelName/message', function(request, response){
    const channelName = request.params.channelName;
    const accountId = rrequesteq.body.accountId;
    const authorization = request.body.authorization;
    const data = request.body.data;
    const req = require('request');
    const options = {
        url: 'https://dev.onair-backend.moon42.com/api/business-layer/v1/chat/account/' + accountId + '/channel/' + channelName,
        headers: {
            'authorization': authorization
        }
    };

    const queryNewMessage = {
        text : "INSERT INTO message(message_id, account_id, data, date_of_creation, channel_name) VALUES($1, $2, $3, $4, $5)",
        values : [uuidv4(), accountId, data, new Date(Date.now()), channelName]
    }

    req.get(options, function(err, res, body){
        if(!err && res.statusCode == 200){
            const responseJson = JSON.parse(body);
            if(responseJson.canWrite){
                if(sockets.get(channelName) != undefined) sockets.get(channelName).emit('sendMessage', data);
                pool.connect((err, client, done) => {
                    if (shouldAbort(err, client, done)) return
                    client.query('BEGIN', (err) => {
                        if (shouldAbort(err, client, done)) return
                        client.query(queryNewMessage, (err, res) => {
                            if (shouldAbort(err, client, done)) return
                            client.query('COMMIT', (err) => {
                                if (err) console.error('Error committing transaction: ', err.message);
                                else console.log("Message saved.");
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

app.patch('/channel/by-name/:channelName', function(request, response){
    const channelName = request.params.channelName;
    const channelStatus = request.body.status;
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
                    if (err) console.error('Error committing transaction: ', err.message);
                    done()
                    response.sendStatus(200);
                    console.log("Channel status updated.");
                })
            })
        })
    })
})

var sockets = new Map();

var sessionData = new Map();

app.get('/channel/:channelName', function(request, response){
    
    const channelName = request.params.channelName;

    const  queryCheckIfChannelExists = {
        text: 'SELECT * FROM channel WHERE channel_name = $1',
        values: [channelName] 
    }

    pool.connect((err, client, done) => {
        if (shouldAbort(err, client, done)) return
        client.query('BEGIN', (err) => {
            if (shouldAbort(err, client, done)) return
            client.query(queryCheckIfChannelExists, (err, res) => {
                if (shouldAbort(err, client, done)){
                    response.sendStatus(500);
                    return
                }
                else if(res.rowCount != 0){
                    response.sendFile(__dirname + '/public/index.html');
                    if(sockets.get(channelName) === undefined){
                        console.log("There is no socket for the channel! Creating...");
                        const socket = require('socket.io')(http)
                        socket.on('connection', function(session){
                            console.log('User connected. Session ID: ' + session.id);
                            session.on('authorization', function(authorizationJson){
                                const authorizationRequest = require('request');
                                const options = {
                                    url:    'https://dev.onair-backend.moon42.com/api/business-layer/v1/chat/account/' + 
                                            authorizationJson.accountId + 
                                            '/channel/' + 
                                            channelName,
                                    headers: {
                                        'authorization': authorizationJson.authorization
                                    }
                                };
                                authorizationRequest.get(options, function(error, apiResponse, body){
                                    if(!error && apiResponse.statusCode == 200){
                                        const responseJson = JSON.parse(body);
                                        sessionData.set(session.id, responseJson);
                                        if(sessionData.get(session.id).canRead){
                                            session.send('<h1>Welcome ' + responseJson.displayName + '!</h1>')
                                            session.broadcast.emit('loginMessage', responseJson.displayName + ' connected.')
                                            const listChannelMessagesRequest = require('request');
                                            const options = {
                                                url: 'http://localhost:8080/channel/by-name/' + channelName + '/messages/?minutes=30'
                                            }
                                            listChannelMessagesRequest.get(options, function(error, listChannelMessagesResponse, body){
                                                if(!error && listChannelMessagesResponse.statusCode == 200){
                                                    const responseJson = JSON.parse(body);
                                                    responseJson.forEach(function(i){ 
                                                        session.send(   sessionData.get(session.id).displayName + 
                                                                        ' on ' + 
                                                                        new Date(Date.parse(i.date_of_creation)) + 
                                                                        ': ' + 
                                                                        i.data) 
                                                    });
                                                }
                                                else{
                                                    response.sendStatus(listChannelMessagesResponse.statusCode)
                                                    console.log(listChannelMessagesResponse.statusCode);
                                                }
                                            })
                                        }
                                        else{
                                            session.send("NOT AUTHORIZED TO READ");
                                            console.log("USER NOT AUTHORIZED TO READ");
                                        }
                                    }
                                    else{
                                        session.send("NOT AUTHORIZED");
                                        console.log("USER NOT AUTHORIZED");
                                    }
                                })
                            })
                            session.on('newMessage', function(data){
                                if(sessionData.get(session.id) !== undefined && sessionData.get(session.id).canWrite){
                                    socket.emit('sendMessage', {'displayName': sessionData.get(session.id).displayName, 'data': data});
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
                                                client.query('COMMIT', (err) => {
                                                    if (err) console.error('Error committing transaction', err.message);
                                                    else console.log("Message saved.");
                                                    done()
                                                })
                                            })
                                        })
                                    })
                                }
                                else{
                                    session.send("NOT AUTHORIZED TO WRITE");
                                    console.log("USER NOT AUTHORIZED TO WRITE");
                                }
                            })
                            session.on('disconnect', function(){
                                console.log('User disconnected.');
                                sessionData.delete(session.id);
                            })
                            sockets.set(channelName, socket);
                        })
                    }
                    else console.log("Socket already exists!");
                    done()
                }
                else{
                    done()
                    response.sendStatus(404);
                }
            })
        })
    })
});
http.listen(8080, function(){ console.log('Listening on localhost:8080') });