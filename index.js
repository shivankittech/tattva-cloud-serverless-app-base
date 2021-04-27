const http = require('http');
const express = require('express');
const { ApolloServer, gql } = require('apollo-server-express');
const { PubSub } = require('apollo-server-express');
const { makeExecutableSchema } = require('graphql-tools');

const MongoClient = require('mongodb').MongoClient;
const pubsub = new PubSub();

const config = require('./config');
let db = null;
const connString = config.mongoDb.connectionUri;
const database = connString.split('/')[3].split('?')[0];
const collection = config.mongoDb.collection;


async function connectDb() {
    try {
        const dbClient = new MongoClient(connString, {
          useNewUrlParser: true,
          useUnifiedTopology: true,
        });
        if (!dbClient.isConnected()) await dbClient.connect();
        return dbClient.db(database);
    } catch (e) {
        console.log('--->error while connecting via graphql context (db)', e)
    }
    return null;
}

async function startBaseServer() {

  const typeDefs = gql`
    type SensorData {
        _id: ID!
        Percentage_Filled: Int
        status: String
        deviceId: String
        createdAt: Int
    }

    type Query {
        sensorData: [SensorData]!
    }

    type Subscription {
        sensorData: SensorData
    }
    `;

    const resolvers = {
        Query: {
        sensorData(_parent, _args, _context, _info) {
            return _context.db
            .collection(collection)
            .find().toArray()
            .then((data) => {
                return data
            });
        },
        },
        Subscription: {
            sensorData: {
              // More on pubsub below
              subscribe: () => pubsub.asyncIterator(['NEW_DATA_RECEIVED']),
            },
        }
    };

  const schema = makeExecutableSchema({
    typeDefs,
    resolvers,
  });

    db = await connectDb();
    const changeStream = db.collection(collection).watch();
    
    changeStream.on('change', (next) => {
        console.log(next.fullDocument);
        pubsub.publish('NEW_DATA_RECEIVED', {
            sensorData: next.fullDocument
        });
    });
  
  const server = new ApolloServer({
    schema,
    context: async () => {
      if (!db) {
        // connect if needed.
        db = await connectDb();
      }
      return { db }
    },
    subscriptions: {
        onConnect: (connectionParams, webSocket, context) => {
        //   console.log('Connected!')
        },
        onDisconnect: (webSocket, context) => {
        //   console.log('Disconnected!')
        },
      },
  });

  await server.start();
  const PORT = 4000;
  const app = express();
  server.applyMiddleware({ app });

  const httpServer = http.createServer(app);
  server.installSubscriptionHandlers(httpServer);
  await new Promise(resolve => httpServer.listen(PORT, resolve));
  console.log(`🚀 Server ready at http://localhost:${PORT}${server.graphqlPath}`);
  console.log(`🚀 Subscriptions ready at ws://localhost:${PORT}${server.subscriptionsPath}`);
  return { server, app, httpServer };
}

startBaseServer();
