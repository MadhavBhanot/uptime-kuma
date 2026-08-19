const { after, describe, test } = require("node:test");
const assert = require("node:assert");
const grpc = require("@grpc/grpc-js");
const protoLoader = require("@grpc/proto-loader");
const { GrpcKeywordMonitorType } = require("../../../server/monitor-types/grpc");
const { UP, PENDING } = require("../../../src/util");
const fs = require("fs");
const path = require("path");
const os = require("os");

const testProto = `
syntax = "proto3";
package test;

service TestService {
    rpc Echo (EchoRequest) returns (EchoResponse);
}

message EchoRequest {
    string message = 1;
}

message EchoResponse {
    string message = 1;
}
`;

// Proto content is constant, so write it once for the whole file
const protoDir = fs.mkdtempSync(path.join(os.tmpdir(), "uptime-kuma-grpc-"));
const protoPath = path.join(protoDir, "test.proto");
fs.writeFileSync(protoPath, testProto);
after(() => fs.rmSync(protoDir, { recursive: true, force: true }));

/**
 * Create a gRPC server for testing
 * @param {object} methodHandlers Object with method handlers
 * @returns {Promise<{server: grpc.Server, port: number}>} gRPC server instance and the port it bound to
 */
async function createTestGrpcServer(methodHandlers) {
    // Load proto file
    const packageDefinition = protoLoader.loadSync(protoPath, {
        keepCase: true,
        longs: String,
        enums: String,
        defaults: true,
        oneofs: true,
    });
    const protoDescriptor = grpc.loadPackageDefinition(packageDefinition);
    const testPackage = protoDescriptor.test;

    const server = new grpc.Server();

    // Add service implementation
    server.addService(testPackage.TestService.service, {
        Echo: (call, callback) => {
            if (methodHandlers.Echo) {
                methodHandlers.Echo(call, callback);
            } else {
                callback(null, { message: call.request.message });
            }
        },
    });

    return new Promise((resolve, reject) => {
        // Bind to port 0 so the OS picks a free port, fixed ports inside the ephemeral
        // range collide with other test files running in parallel under `node --test`
        server.bindAsync("0.0.0.0:0", grpc.ServerCredentials.createInsecure(), (err, port) => {
            if (err) {
                reject(err);
            } else {
                server.start();
                resolve({ server, port });
            }
        });
    });
}

describe(
    "GrpcKeywordMonitorType",
    {
        skip: !!process.env.CI && (process.platform !== "linux" || process.arch !== "x64"),
    },
    () => {
        test("check() sets status to UP when keyword is found in response", async () => {
            const { server, port } = await createTestGrpcServer({
                Echo: (call, callback) => {
                    callback(null, { message: "Hello World with SUCCESS keyword" });
                },
            });

            const grpcMonitor = new GrpcKeywordMonitorType();
            const monitor = {
                grpcUrl: `localhost:${port}`,
                grpcProtobuf: testProto,
                grpcServiceName: "test.TestService",
                grpcMethod: "echo",
                grpcBody: JSON.stringify({ message: "test" }),
                keyword: "SUCCESS",
                invertKeyword: false,
                grpcEnableTls: false,
                isInvertKeyword: () => false,
            };

            const heartbeat = {
                msg: "",
                status: PENDING,
            };

            try {
                await grpcMonitor.check(monitor, heartbeat, {});
                assert.strictEqual(heartbeat.status, UP);
                assert.ok(heartbeat.msg.includes("SUCCESS"));
                assert.ok(heartbeat.msg.includes("is"));
            } finally {
                server.forceShutdown();
            }
        });

        test("check() rejects when keyword is not found in response", async () => {
            const { server, port } = await createTestGrpcServer({
                Echo: (call, callback) => {
                    callback(null, { message: "Hello World without the expected keyword" });
                },
            });

            const grpcMonitor = new GrpcKeywordMonitorType();
            const monitor = {
                grpcUrl: `localhost:${port}`,
                grpcProtobuf: testProto,
                grpcServiceName: "test.TestService",
                grpcMethod: "echo",
                grpcBody: JSON.stringify({ message: "test" }),
                keyword: "MISSING",
                invertKeyword: false,
                grpcEnableTls: false,
                isInvertKeyword: () => false,
            };

            const heartbeat = {
                msg: "",
                status: PENDING,
            };

            try {
                await assert.rejects(grpcMonitor.check(monitor, heartbeat, {}), (err) => {
                    assert.ok(err.message.includes("MISSING"));
                    assert.ok(err.message.includes("not"));
                    return true;
                });
            } finally {
                server.forceShutdown();
            }
        });

        test("check() rejects when inverted keyword is present in response", async () => {
            const { server, port } = await createTestGrpcServer({
                Echo: (call, callback) => {
                    callback(null, { message: "Response with ERROR keyword" });
                },
            });

            const grpcMonitor = new GrpcKeywordMonitorType();
            const monitor = {
                grpcUrl: `localhost:${port}`,
                grpcProtobuf: testProto,
                grpcServiceName: "test.TestService",
                grpcMethod: "echo",
                grpcBody: JSON.stringify({ message: "test" }),
                keyword: "ERROR",
                invertKeyword: true,
                grpcEnableTls: false,
                isInvertKeyword: () => true,
            };

            const heartbeat = {
                msg: "",
                status: PENDING,
            };

            try {
                await assert.rejects(grpcMonitor.check(monitor, heartbeat, {}), (err) => {
                    assert.ok(err.message.includes("ERROR"));
                    assert.ok(err.message.includes("present"));
                    return true;
                });
            } finally {
                server.forceShutdown();
            }
        });

        test("check() sets status to UP when inverted keyword is not present in response", async () => {
            const { server, port } = await createTestGrpcServer({
                Echo: (call, callback) => {
                    callback(null, { message: "Response without error keyword" });
                },
            });

            const grpcMonitor = new GrpcKeywordMonitorType();
            const monitor = {
                grpcUrl: `localhost:${port}`,
                grpcProtobuf: testProto,
                grpcServiceName: "test.TestService",
                grpcMethod: "echo",
                grpcBody: JSON.stringify({ message: "test" }),
                keyword: "ERROR",
                invertKeyword: true,
                grpcEnableTls: false,
                isInvertKeyword: () => true,
            };

            const heartbeat = {
                msg: "",
                status: PENDING,
            };

            try {
                await grpcMonitor.check(monitor, heartbeat, {});
                assert.strictEqual(heartbeat.status, UP);
                assert.ok(heartbeat.msg.includes("ERROR"));
                assert.ok(heartbeat.msg.includes("not"));
            } finally {
                server.forceShutdown();
            }
        });

        test("check() rejects when gRPC server is unreachable", async () => {
            const grpcMonitor = new GrpcKeywordMonitorType();
            const monitor = {
                grpcUrl: "localhost:50099",
                grpcProtobuf: testProto,
                grpcServiceName: "test.TestService",
                grpcMethod: "echo",
                grpcBody: JSON.stringify({ message: "test" }),
                keyword: "SUCCESS",
                invertKeyword: false,
                grpcEnableTls: false,
                isInvertKeyword: () => false,
            };

            const heartbeat = {
                msg: "",
                status: PENDING,
            };

            await assert.rejects(grpcMonitor.check(monitor, heartbeat, {}), (err) => {
                // Should fail with connection error
                return true;
            });
        });

        test("check() truncates long response messages in error output", async () => {
            const longMessage = "A".repeat(100) + " with SUCCESS keyword";

            const { server, port } = await createTestGrpcServer({
                Echo: (call, callback) => {
                    callback(null, { message: longMessage });
                },
            });

            const grpcMonitor = new GrpcKeywordMonitorType();
            const monitor = {
                grpcUrl: `localhost:${port}`,
                grpcProtobuf: testProto,
                grpcServiceName: "test.TestService",
                grpcMethod: "echo",
                grpcBody: JSON.stringify({ message: "test" }),
                keyword: "MISSING",
                invertKeyword: false,
                grpcEnableTls: false,
                isInvertKeyword: () => false,
            };

            const heartbeat = {
                msg: "",
                status: PENDING,
            };

            try {
                await assert.rejects(grpcMonitor.check(monitor, heartbeat, {}), (err) => {
                    // Should truncate message to 50 characters with "..."
                    assert.ok(err.message.includes("..."));
                    return true;
                });
            } finally {
                server.forceShutdown();
            }
        });
    }
);
