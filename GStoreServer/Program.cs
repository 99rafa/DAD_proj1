using Grpc.Core;
using Grpc.Net.Client;
using System;
using System.Collections.Generic;
using System.IO;
using System.Security;
using System.Threading;
using System.Threading.Tasks;
using System.Text.RegularExpressions;
using System.Runtime.CompilerServices;
using System.Linq;

namespace gStoreServer {
    public struct ServerStruct {
        public string url;
        public GStoreServerService.GStoreServerServiceClient service;

        public ServerStruct(string u, GStoreServerService.GStoreServerServiceClient s) {
            url = u;
            service = s;
        }
    }
    public class PuppetServerService : PuppetMasterService.PuppetMasterServiceBase {
        public String url;
        //List of servers in each partition, if an entry for a partition exists then this server belongs to that partition
        public Dictionary<String, List<ServerStruct>> partitionServers = new Dictionary<String, List<ServerStruct>>();
        

        public PuppetServerService(String h) {
            url = h;
        }

        public override Task<PartitionReply> Partition(PartitionRequest request, ServerCallContext context) {
            Console.WriteLine("Received partition request: partition_name: " + request.PartitionName);
            if (!partitionServers.ContainsKey(request.PartitionName)) {
                partitionServers.Add(request.PartitionName, new List<ServerStruct>());
            } else {
                Console.WriteLine("Received partition creation request that already exists: " + request.PartitionName);
            }
            for(int i = 0; i < request.ServersUrls.Count; i++) {
                //Add to replicaMaster if this server is the master
                if (i==0 && request.ServersUrls[i].Equals(url)) {
                    Console.WriteLine("\tThis Server: " + url + "  is master of partition: " + request.PartitionName);
                }
                // add server url to the list of servers from this partition
                Console.WriteLine("\tAdded server " + request.ServersUrls[i] +  " to local list of partition servers: " + request.PartitionName);
                GrpcChannel channel = GrpcChannel.ForAddress("http://" + request.ServersUrls[i]);
                GStoreServerService.GStoreServerServiceClient service = new GStoreServerService.GStoreServerServiceClient(channel);
                partitionServers[request.PartitionName].Add(new ServerStruct(request.ServersUrls[i], service));
                
            }
            return Task.FromResult(new PartitionReply {
                Ok = true
            });

        }

        public override Task<StatusReply> Status(StatusRequest request, ServerCallContext context)
        {

            Console.WriteLine("Status request received!! Server at port: " + url + " running...");

            return Task.FromResult(new StatusReply
            {
                Ok = true
            });
            
        }

        public bool serverIsMaster(String partitionName) {
            if (partitionServers.ContainsKey(partitionName)) {
                return partitionServers[partitionName][0].url == url;
            }
            return false;
        }

        public List<ServerStruct> getPartitionServers(String partitionName) {
            return partitionServers[partitionName];
        }

        public override Task<CrashReply> Crash(CrashRequest request, ServerCallContext context)
        {

            Console.WriteLine("Crash request received!!");
            Console.WriteLine("Crashing...");

            return Task.FromResult(new CrashReply
            {
                Ok = true
            });

        }

        public override Task<FreezeReply> Freeze(FreezeRequest request, ServerCallContext context)
        {

            Console.WriteLine("Freeze request received!!");

            return Task.FromResult(new FreezeReply
            {
                Ok = true
            });

        }

        public override Task<UnfreezeReply> Unfreeze(UnfreezeRequest request, ServerCallContext context)
        {

            Console.WriteLine("Unfreeze request received!!");

            return Task.FromResult(new UnfreezeReply
            {
                Ok = true
            });

        }
    }
    // GStoreServerService is the namespace defined in the protobuf
    // GStoreServerServiceBase is the generated base implementation of the service
    public class ServerService : GStoreServerService.GStoreServerServiceBase{
        private GrpcChannel channel;
        private Dictionary<string, GStoreClientService.GStoreClientServiceClient> clientMap =
            new Dictionary<string, GStoreClientService.GStoreClientServiceClient>();
        public PuppetServerService puppetService;
        private Dictionary<Tuple<String, String>, String> serverObjects = new Dictionary<Tuple<String, String>, String>();

        private Semaphore _semaphore = new Semaphore(1, 1);

        public ServerService(PuppetServerService p) {
            puppetService = p;
        }
        //public override Task<ReadValueReply> ReadValue(GStoreClientRegisterRequest request, ServerCallContext context) {

        //}

        public override Task<ListGlobalReply> ListGlobal(ListGlobalRequest request, ServerCallContext context)
        {
            Console.WriteLine("Sending all stored objects...");

            _semaphore.WaitOne();
            ListGlobalReply reply = new ListGlobalReply { };
            foreach(var pair in serverObjects)
            {
                reply.ObjDesc.Add(new ObjectDescription { ObjectId = pair.Key.Item2,PartitionId = pair.Key.Item1});
            }
            _semaphore.Release();
            return Task.FromResult(reply);
        }

        public override Task<ListServerObjectsReply> ListServerObjects(ListServerObjectsRequest request, ServerCallContext context)
        {
            ListServerObjectsReply reply = new ListServerObjectsReply { };
            foreach(var pair in serverObjects)
            {
                String obj_id = pair.Key.Item2;
                String part_id = pair.Key.Item1;
                String val = pair.Value;

                reply.Objects.Add(new Object { ObjectId = obj_id,
                                               PartitionId = part_id,
                                               Value = val,
                                               IsMaster = puppetService.serverIsMaster(part_id)
                                             });
            }
            return Task.FromResult(reply);
        }

        public async override Task<WriteValueReply> WriteValue(WriteValueRequest request, ServerCallContext context) {
            Console.WriteLine("Received write request for partition " + request.PartitionId + " on objet " + request.ObjectId + " with value " + request.Value);
            //Check if this server is master of partition
            if (!puppetService.serverIsMaster(request.PartitionId)) {
                return await Task.FromResult(new WriteValueReply {
                    Ok = false
                });
            }
            

            _semaphore.WaitOne();
            try { 
                serverObjects.Add(new Tuple<string, string>(request.PartitionId, request.ObjectId), request.Value);

                //Sends lock request to every server in partition and wait for all acks
                List<AsyncUnaryCall<LockReply>> pendingLocks = new List<AsyncUnaryCall<LockReply>>();
                foreach (ServerStruct server in puppetService.getPartitionServers(request.PartitionId)) {
                    if(server.url != puppetService.url) {
                        Console.WriteLine("\t\tSending lock request to " + server.url);
                        AsyncUnaryCall<LockReply> reply = server.service.LockAsync(new LockRequest { });
                        pendingLocks.Add(reply);
                    }
                    
                }

                //wait for all LOCK responses
                await Task.WhenAll(pendingLocks.Select(c => c.ResponseAsync));
                Console.WriteLine("\tLock requests completed");
                
                //Share write with all replicas
                List<AsyncUnaryCall<ShareWriteReply>> pendingTasks = new List<AsyncUnaryCall<ShareWriteReply>>();
                foreach(ServerStruct server in puppetService.getPartitionServers(request.PartitionId)) {
                    if (server.url != puppetService.url) {
                        Console.WriteLine("\t\tSending write share request to " + server.url);
                        AsyncUnaryCall<ShareWriteReply> reply = server.service.ShareWriteAsync(new ShareWriteRequest {
                            PartitionId = request.PartitionId,
                            ObjectId = request.ObjectId,
                            Value = request.Value
                        });
                        pendingTasks.Add(reply);
                    }
                }

                //wait for all WRITE SHARE responses
                await Task.WhenAll(pendingTasks.Select(c => c.ResponseAsync));
                Console.WriteLine("\tSharing writes completed");
                Console.WriteLine("Write in partition completed");
            } finally {
                _semaphore.Release();
            }

            return await Task.FromResult(new WriteValueReply {
                Ok = true
            });
        }

        public override Task<ShareWriteReply> ShareWrite(ShareWriteRequest request, ServerCallContext context) {
            Console.WriteLine("Received shared write :   objId: " + request.ObjectId + "    value: " + request.Value);
            try {
                //write object
                serverObjects[new Tuple<string, string>(request.PartitionId, request.ObjectId)] = request.Value; 
            }
            finally {
                //Release lock
                _semaphore.Release();
            }
            Console.WriteLine("Released Lock");

            return Task.FromResult(new ShareWriteReply {
                Ok = true
            });
        }

        public override Task<LockReply> Lock(LockRequest request, ServerCallContext context) {
            //Lock 
            Console.WriteLine("Locking server for write ");
            _semaphore.WaitOne();
            return Task.FromResult(new LockReply {
                Ok = true
            });
        }

        public override Task<ReadValueReply> ReadValue(ReadValueRequest request, ServerCallContext context) {
            Console.WriteLine("Received Read request ");
            string value;
            _semaphore.WaitOne();
            Tuple<string, string> key = new Tuple<string, string>(request.PartitionId, request.ObjectId);
            if (serverObjects.ContainsKey(key)){
                value = serverObjects[key];
            } else {
                value = "N/A";
            }
            _semaphore.Release();
            return Task.FromResult(new ReadValueReply {
                Value = value
            });
        }

        public override Task<GStoreClientRegisterReply> Register(
            GStoreClientRegisterRequest request, ServerCallContext context) {
            Console.WriteLine("Deadline: " + context.Deadline);
            Console.WriteLine("Host: " + context.Host);
            Console.WriteLine("Method: " + context.Method);
            Console.WriteLine("Peer: " + context.Peer);
            return Task.FromResult(Reg(request));
        }

        public override Task<BcastMsgReply> BcastMsg(BcastMsgRequest request, ServerCallContext context) {
            return Task.FromResult(Bcast(request));
        }


        public BcastMsgReply Bcast(BcastMsgRequest request) {
            // random wait to simulate slow msg broadcast: Thread.Sleep(5000);
            Console.WriteLine("msg arrived. lazy server waiting for server admin to press key.");
            Console.ReadKey();
            lock (this) {
                foreach (string nick in clientMap.Keys) {
                    if (nick != request.Nick) {
                        try {
                            clientMap[nick].RecvMsg(new RecvMsgRequest
                            {
                                Msg = request.Nick + ": " + request.Msg 
                            });
                        } catch (Exception e) {
                            Console.WriteLine(e.Message);
                            clientMap.Remove(nick);
                        }
                    }
                }
            }
            Console.WriteLine($"Broadcast message {request.Msg} from {request.Nick}");
            return new BcastMsgReply
            {
                Ok = true
            };
        }

        public GStoreClientRegisterReply Reg(GStoreClientRegisterRequest request) {
                channel = GrpcChannel.ForAddress(request.Url);
                GStoreClientService.GStoreClientServiceClient client =
                    new GStoreClientService.GStoreClientServiceClient(channel);
            lock (this) {
                clientMap.Add(request.Nick, client);
            }
            Console.WriteLine($"Registered client {request.Nick} with URL {request.Url}");
            GStoreClientRegisterReply reply = new GStoreClientRegisterReply();
            lock (this) {
                foreach (string nick in clientMap.Keys) {
                    reply.Users.Add(new User { Nick = nick });
                }
            }
            return reply;
        }
    }

    //Client side of the server
    //public class ServerService : GStoreServerService.GStoreServerServiceClient {
        class Program {
        
        public static void Main(string[] args) {
            //const int port = 1001;
            //const string hostname = "localhost";
            string startupMessage;
            ServerPort serverPort;

            String hostname = Regex.Matches(args[1], "[A-Za-z]+[^:]")[0].ToString();
            int port = int.Parse(Regex.Matches(args[1], "[^:]*[0-9]+")[0].ToString());
            int min_delay = int.Parse(args[2]);
            int max_delay = int.Parse(args[3]);

            serverPort = new ServerPort(hostname, port, ServerCredentials.Insecure);
            startupMessage = "Insecure GStoreServer server listening on port " + port;

            PuppetServerService puppetService = new PuppetServerService(hostname + ":" + port);
            ServerService serverService = new ServerService(puppetService);

            Server server = new Server
            {
                Services = { GStoreServerService.BindService(serverService),
                             PuppetMasterService.BindService(puppetService)},
                Ports = { serverPort }
            };

            server.Start();

            Console.WriteLine(startupMessage);
            Console.WriteLine("Server_id: " + args[0] + "\t hostname: " + hostname + "\t min_delay: "+min_delay+"\t max_delay: "+ max_delay);
            //Configuring HTTP for client connections in Register method
            AppContext.SetSwitch(
  "System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);
            while (true) ;
        }
    }
}

