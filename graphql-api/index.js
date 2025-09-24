const { ApolloServer } = require('@apollo/server');
const { startStandaloneServer } = require('@apollo/server/standalone');
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');

// Load gRPC proto
const packageDef = protoLoader.loadSync("../grpc-protos/metrics.proto", {});
const grpcObject = grpc.loadPackageDefinition(packageDef);
const client = new grpcObject.logpipeline.LogPipelineService(
  "localhost:50051",
  grpc.credentials.createInsecure()
);

// GraphQL schema
const typeDefs = `#graphql
  type Metrics {
    endpoint: String
    totalRequests: Int
    errorCount: Int
    errorRate: Float
  }

  type Alert {
    endpoint: String
    severity: String
    errorRate: Float
    triggeredAt: String
  }

  type LogEntry {
    timestamp: String
    level: String
    endpoint: String
    message: String
  }

  type Query {
    metrics(endpoint: String!): Metrics
    alerts(active: Boolean): [Alert]
    logs(limit: Int, level: String): [LogEntry]
  }
`;

// Resolvers with debug logs
const resolvers = {
  Query: {
    metrics: (_, { endpoint }) =>
      new Promise((resolve, reject) => {
        client.GetMetrics({ endpoint }, (err, res) => {
          console.log("[GraphQL] GetMetrics response:", res, "error:", err);
          if (err) reject(err);
          else resolve(res);
        });
      }),

    alerts: (_, { active }) =>
      new Promise((resolve, reject) => {
        client.GetAlerts({ active }, (err, res) => {
          console.log("[GraphQL] GetAlerts response:", res, "error:", err);
          if (err) reject(err);
          else resolve(res.alerts);
        });
      }),

    logs: (_, { limit, level }) =>
      new Promise((resolve, reject) => {
        client.GetLogs({ limit, level }, (err, res) => {
          console.log("[GraphQL] GetLogs response:", res, "error:", err);
          if (err) reject(err);
          else resolve(res.logs);
        });
      }),
  },
};

async function start() {
  const server = new ApolloServer({ typeDefs, resolvers });
  const { url } = await startStandaloneServer(server, { listen: { port: 4000 } });
  console.log(`🚀 GraphQL API ready at ${url}`);
}

start();
