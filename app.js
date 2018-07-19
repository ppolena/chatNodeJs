var app = require('express')();
var http = require('http').Server(app);
var url = require('url');
const uuidv4 = require('uuid/v4');

const { Pool, Client } = require('pg');

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

pool.connect((err, client, done) => {

    const shouldAbort = (err) => {
        if (err) {
            console.error('Error in transaction!', err.stack)
            client.query('ROLLBACK', (err) => {
                if (err) {
                console.error('Error rolling back client!', err.stack)
                }
                done()
            })
        }
        return !!err
    }

    client.query('BEGIN', (err) => {
        if (shouldAbort(err)) return
        client.query(queryCheckIfChannelTableExists, (err, res) => {
            if (shouldAbort(err)) return
            if(res.rows[0].to_regclass == null){
                console.log("Channel table doesn't exist. Creating...");
                client.query(queryCreateChannelTable, (err, res) => {
                    if (shouldAbort(err)) return
                    client.query(queryCheckIfMessageTableExists, (err, res) => {
                        if (shouldAbort(err)) return
                        if(res.rows[0].to_regclass == null){
                            console.log("Message table doesn't exist. Creating...");
                            client.query(queryCreateMessageTable, (err,res) => {
                                if (shouldAbort(err)) return
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
                    if (shouldAbort(err)) return
                    if(res.rows[0].to_regclass == null){
                        console.log("Message table doesn't exist. Creating...");
                        client.query(queryCreateMessageTable, (err,res) => {
                            if (shouldAbort(err)) return
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

var sockets = new Map();

app.get('/channel/:channel_name', function(req, res){
    res.sendFile(__dirname + '/public/index.html');
    
    const channelName = req.params.channel_name;
    console.log(channelName);

    const queryCheckIfChannelExists = {
        text: "SELECT * FROM channel WHERE channel_name = $1",
        values: [channelName]
    }

    const queryCreateChannel = {
        text: 'INSERT INTO channel(channel_name, status, date_of_creation, date_of_closing) VALUES($1, $2, $3, $4)',
        values: [channelName, "1", new Date(Date.now()), null]
    }

    pool.connect((err, client, done) => {

        const shouldAbort = (err) => {
            if (err) {
                console.error('Error in transaction!', err.stack);
                client.query('ROLLBACK', (err) => {
                    if (err) {
                        console.error('Error rolling back client!', err.stack);
                    }
                    done()
                })
            }
            return !!err
        }

        client.query('BEGIN', (err) => {
            if (shouldAbort(err)) return
            client.query(queryCheckIfChannelExists, (err, res) => {
                if (shouldAbort(err)) return
                if(res.rowCount == 0){
                    console.log("Channel doesn't exist. Creating...");
                    client.query(queryCreateChannel, (err, res) => {
                        if (shouldAbort(err)) return
                        client.query('COMMIT', (err) => {
                            if (err) {
                                console.error('Error committing transaction', err.stack);
                            }
                            done()
                        })
                    })
                }
                else{
                    console.log("Channel already exists!");
                    done()
                }
            })
        })
    })

    if(sockets.get(channelName) === undefined){
            console.log("There is no socket for the channel! Creating...");
            const io = require('socket.io')(http)
            io.on('connection', function(socket){
                console.log('User connected.');
                socket.on('disconnect', function(){
                    console.log('User disconnected.');
                })
                socket.on('newMessage', function(message){
                    io.emit('sendMessage', message.data)
                    const queryNewMessage = {
                        text : "INSERT INTO message(message_id, account_id, data, date_of_creation, channel_name) VALUES($1, $2, $3, $4, $5)",
                        values : [uuidv4(), message.accountId, message.data, new Date(Date.now()), channelName]
                    }
                    pool.connect((err, client, done) => {

                        const shouldAbort = (err) => {
                            if (err) {
                                console.error('Error in transaction!', err.stack);
                                client.query('ROLLBACK', (err) => {
                                    if (err) {
                                        console.error('Error rolling back client!', err.stack);
                                    }
                                    done()
                                })
                            }
                            return !!err
                        }

                        client.query('BEGIN', (err) => {
                            if (shouldAbort(err)) return
                            client.query(queryNewMessage, (err, res) => {
                                if (shouldAbort(err)) return
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
