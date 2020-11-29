using Grpc.Core;
using Grpc.Net.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf.Collections;

namespace gStoreServer
{
    public struct ServerStruct
    {
        public string url;
        public GStoreServerService.GStoreServerServiceClient service;

        public ServerStruct(string u, GStoreServerService.GStoreServerServiceClient s)
        {
            url = u;
            service = s;
        }
    }
    public class PuppetServerService : PuppetMasterService.PuppetMasterServiceBase
    {
        public String url;
        //List of servers in each partition, if an entry for a partition exists then this server belongs to that partition
        public Dictionary<String, List<ServerStruct>> partitionServers = new Dictionary<String, List<ServerStruct>>();
        public Dictionary<String, GStoreServerService.GStoreServerServiceClient> openChannels = new Dictionary<String, GStoreServerService.GStoreServerServiceClient>();

        public bool crashed = false;
        public bool frozen = false;



        public PuppetServerService(String h)
        {
            url = h;
        }

        public override Task<PartitionReply> Partition(PartitionRequest request, ServerCallContext context)
        {
            Console.WriteLine("Received partition request: partition_name: " + request.PartitionName);
            if (!partitionServers.ContainsKey(request.PartitionName))
            {
                partitionServers.Add(request.PartitionName, new List<ServerStruct>());
            }
            else
            {
                Console.WriteLine("Received partition creation request that already exists: " + request.PartitionName);
            }
            for (int i = 0; i < request.ServersUrls.Count; i++)
            {
                //Add to replicaMaster if this server is the master
                string server_url = request.ServersUrls[i];
                if (i == 0 && server_url.Equals(url))
                {
                    Console.WriteLine("\tThis Server: " + url + "  is master of partition: " + request.PartitionName);
                }
                // add server url to the list of servers from this partition
                // check openChannels first to avoid creating two channels for the same server
                try
                {
                    if (!openChannels.ContainsKey(server_url))
                    {
                        GrpcChannel channel = GrpcChannel.ForAddress("http://" + request.ServersUrls[i]);
                        GStoreServerService.GStoreServerServiceClient service = new GStoreServerService.GStoreServerServiceClient(channel);
                        openChannels.Add(request.ServersUrls[i], service);
                    }
                    Console.WriteLine("\tAdded server " + request.ServersUrls[i] + " to local list of partition servers: " + request.PartitionName);
                    partitionServers[request.PartitionName].Add(new ServerStruct(server_url, openChannels[server_url]));
                }
                catch (RpcException)
                {
                    Console.WriteLine("Connection failed to server " + request.ServersUrls[i]);
                    removeServer(request.ServersUrls[i]);
                }

            }
            return Task.FromResult(new PartitionReply
            {
                Ok = true
            });

        }

        public override Task<StatusReply> Status(StatusRequest request, ServerCallContext context)
        {

            Console.WriteLine("Status request received! Server at port: " + url + " running...");

            return Task.FromResult(new StatusReply
            {
                Ok = true
            });

        }

        public bool serverIsMaster(String partitionName)
        {
            if (partitionServers.ContainsKey(partitionName))
            {
                return partitionServers[partitionName][0].url == url;
            }
            return false;
        }

        public List<ServerStruct> getPartitionServers(String partitionName)
        {
            return partitionServers[partitionName];
        }

        public override Task<CrashReply> Crash(CrashRequest request, ServerCallContext context)
        {

            Console.WriteLine("Crashing...");

            crashed = true;

            return Task.FromResult(new CrashReply
            {
                Ok = true
            });

        }

        public override Task<FreezeReply> Freeze(FreezeRequest request, ServerCallContext context)
        {

            Console.WriteLine("Freeze request received!");

            frozen = true;


            return Task.FromResult(new FreezeReply
            {
                Ok = true
            });

        }

        public override Task<UnfreezeReply> Unfreeze(UnfreezeRequest request, ServerCallContext context)
        {

            Console.WriteLine("Unfreeze request received!");

            frozen = false;

            return Task.FromResult(new UnfreezeReply
            {
                Ok = true
            });

        }

        public void removeServer(string url) {

            openChannels.Remove(url);
            List<string> removals = new List<string>();
            foreach (KeyValuePair<string, List<ServerStruct>> partition in partitionServers) {
                if (partition.Value.Any(server => server.url == url)) {
                    removals.Add(partition.Key);
                }
            }
            foreach(string remove in removals) {
                partitionServers[remove] = partitionServers[remove].Where(server => server.url != url).ToList();
            }
        }
    }
    // GStoreServerService is the namespace defined in the protobuf
    // GStoreServerServiceBase is the generated base implementation of the service
    public class ServerService : GStoreServerService.GStoreServerServiceBase
    {

        public PuppetServerService puppetService;

        //key: <partition_id, object_id>      value: object_value
        private Dictionary<Tuple<String, String>, String> serverObjects = new Dictionary<Tuple<String, String>, String>();

        //timestamp -  key: <partition_id, object_id>      value: <server_id, object_version>
        private Dictionary<Tuple<String, String>, Tuple<int, int>> timestamp = new Dictionary<Tuple<String, String>, Tuple<int, int>>();

        private Semaphore _semaphore = new Semaphore(1, 1);

        int min_delay, max_delay, server_int_id;

        // <partition_name, integer_value> Each value is used as a lock for that partition
        private Dictionary<string, int> partitionLocks;

        public ServerService(PuppetServerService p, int min, int max, int serv_int)
        {
            puppetService = p;
            min_delay = min;
            max_delay = max;
            server_int_id = serv_int;

            foreach (var item in puppetService.partitionServers) {
                partitionLocks.Add(item.Key, 0);
            }
        }


        private int RandomDelay()
        {
            Random r = new Random();
            int delay = r.Next(min_delay, max_delay);
            Console.WriteLine("Adding a " + delay + " ms delay to the request");
            return delay;
        }
        public override Task<ListGlobalReply> ListGlobal(ListGlobalRequest request, ServerCallContext context)
        {
            while (puppetService.frozen) ;
            System.Threading.Thread.Sleep(RandomDelay());
            Console.WriteLine("Sending all stored objects...");


            ListGlobalReply reply = new ListGlobalReply { };
            String partition_id = request.PartitionId;
            foreach (var pair in serverObjects)
            {
                _semaphore.WaitOne();
                if (pair.Key.Item1 == partition_id)
                    reply.ObjDesc.Add(new ObjectDescription { ObjectId = pair.Key.Item2, PartitionId = pair.Key.Item1 });
                _semaphore.Release();
            }

            return Task.FromResult(reply);
        }

        public override Task<ListServerObjectsReply> ListServerObjects(ListServerObjectsRequest request, ServerCallContext context)
        {
            while (puppetService.frozen) ;
            System.Threading.Thread.Sleep(RandomDelay());
            Console.WriteLine("Received listServer request");
            ListServerObjectsReply reply = new ListServerObjectsReply { };
            foreach (var pair in serverObjects)
            {
                _semaphore.WaitOne();
                String obj_id = pair.Key.Item2;
                String part_id = pair.Key.Item1;
                String val = pair.Value;
                Console.WriteLine("\tAdding object " + obj_id + " with value " + val);
                reply.Objects.Add(new Object
                {
                    ObjectId = obj_id,
                    PartitionId = part_id,
                    Value = val,
                    IsMaster = puppetService.serverIsMaster(part_id)
                });
                _semaphore.Release();
            }
            return Task.FromResult(reply);
        }

        public void printTimestamp() {
            Console.WriteLine("Printing Timestamp");
            foreach(var t in timestamp) {
                Console.WriteLine("KEY:  partition:" + t.Key.Item1 + "    object_id:" + t.Key.Item2 );
                Console.WriteLine("VALUE timestamp   server_id:" + t.Value.Item1 + "    object_version:" + t.Value.Item2);
            }
        }

        public async override Task<WriteValueReply> WriteValue(WriteValueRequest request, ServerCallContext context)
        {

            while (puppetService.frozen) ;
            System.Threading.Thread.Sleep(RandomDelay());

            Console.WriteLine("\nReceived write request for partition " + request.PartitionId + " on objet " + request.ObjectId + " with value " + request.Value);

            //LOCK
            Console.WriteLine("Contains " + this.puppetService.partitionServers.ContainsKey(request.PartitionId));
            List<ServerStruct> partitionLock = this.puppetService.partitionServers[request.PartitionId];
            Monitor.Enter(partitionLock);

            int newVersion = 1;
            try {
                //Check if this server is master of partition
                if (!puppetService.serverIsMaster(request.PartitionId)) {
                    Console.WriteLine("\t\t Receiving write request when this server is not the leader. Becoming leader");
                    removeCurrentMaster(request.PartitionId);
                }


                //Change current value
                serverObjects[new Tuple<string, string>(request.PartitionId, request.ObjectId)] = request.Value;

                //increment current timestamp
                Tuple<String, String> receivedObjecID = new Tuple<String, String>(request.PartitionId, request.ObjectId);
                //if(timestamp.Keys.Any(x => x.Item1 == receivedObject.Item1 && x.Item2 == receivedObject.Item2)) { // If the timestamp already contains this object
                //    timestamp[]
                //}

                if (timestamp.ContainsKey(receivedObjecID)) { // increment version if it already has it
                    newVersion = timestamp[receivedObjecID].Item2 + 1;
                    timestamp[receivedObjecID] = new Tuple<int, int>(server_int_id, newVersion);
                }
                else {// otherwise version = 1
                    timestamp[receivedObjecID] = new Tuple<int, int>(server_int_id, newVersion);
                }

                //DEBUG
                printTimestamp();


                //Send values and timestamps to every server in partition

                //TODO MIGHT BE MISSING LOCK ON PARTITION SERVER

                List<AsyncUnaryCall<GossipReply>> pendingRequests = new List<AsyncUnaryCall<GossipReply>>();
                List<ServerStruct> partitionServers = puppetService.getPartitionServers(request.PartitionId);
                foreach (ServerStruct server in partitionServers) {
                    if (server.url != puppetService.url) { // dont send url to itself
                        Console.WriteLine("\t Sending gossip with single write to " + server.url + " on object " + request.ObjectId + " with values " + request.Value);
                        try {
                            GossipRequest gossipRequest = new GossipRequest { };
                            gossipRequest.Timestamp.Add(new timestampValue {
                                PartitionId = request.PartitionId,
                                ObjectId = request.ObjectId,
                                ServerId = server_int_id,
                                ObjectVersion = newVersion,
                                ObjectValue = request.Value
                            });
                            Console.WriteLine("\t Sending gossip " + request.PartitionId + "  " + request.ObjectId + "  " + server_int_id + "  " + newVersion + "  " + request.Value);
                            AsyncUnaryCall<GossipReply> reply = server.service.GossipAsync(gossipRequest);
                            pendingRequests.Add(reply);
                        }
                        catch (RpcException) {
                            Console.WriteLine("Connection failed to server " + server.url + " removing server");
                            puppetService.removeServer(server.url);
                        }

                    }
                }
                //Wait for answers?
                



                } finally { Monitor.Exit(partitionLock); }

            /*


                try {
                    //wait for all LOCK responses
                    await Task.WhenAll(pendingLocks.Select(c => c.ResponseAsync));
                    Console.WriteLine("\tLock requests completed");
                }
                catch (RpcException) {
                    Console.WriteLine("\t\tConnection failed to server, removing server");
                    for (int i = 0; i < pendingLocks.Count(); i++) {
                        if (pendingLocks[i].ResponseAsync.Exception != null) {
                            Console.WriteLine("\t\tRemoving server: " + partitionServers[i + 1].url); //i+1 because the master server is also in the partitionServers list
                            puppetService.removeServer(partitionServers[i + 1].url);
                        }
                    }
                }*/


            return await Task.FromResult(new WriteValueReply
            {
                Ok = true
            });
        }

        public override Task<ShareWriteReply> ShareWrite(ShareWriteRequest request, ServerCallContext context)
        {

            while (puppetService.frozen) ;
            System.Threading.Thread.Sleep(RandomDelay());

            Console.WriteLine("Received shared write :   objId: " + request.ObjectId + "    value: " + request.Value);
            try
            {
                //write object
                serverObjects[new Tuple<string, string>(request.PartitionId, request.ObjectId)] = request.Value;
            }
            finally
            {
                //Release lock
                _semaphore.Release();
            }
            Console.WriteLine("Released Lock");

            return Task.FromResult(new ShareWriteReply
            {
                Ok = true
            });
        }

        public override Task<GossipReply> Gossip(GossipRequest request, ServerCallContext context) {
            //List<ServerStruct> partitionLock = this.puppetService.partitionServers[request.Timestamp[0].PartitionId];
            //Monitor.Enter(partitionLock);
            Console.WriteLine("\nReceived Gossip " + request.Timestamp.Count());
            foreach(timestampValue ts in request.Timestamp) {
                Tuple<String, String> key = new Tuple<String, String>(ts.PartitionId, ts.ObjectId);

                List<ServerStruct> partitionLock = this.puppetService.partitionServers[ts.PartitionId];
                Monitor.Enter(partitionLock);
                Console.WriteLine(" Locking ");
                try {
                    Console.WriteLine("Reading Gossip2 with value" + ts.ObjectValue + "  " + (!this.timestamp.ContainsKey(key)));

                    // Add value to local if the local server doesn't have a timestamp for it or if received version is more recent than the current version
                    if ((!this.timestamp.ContainsKey(key)) || (ts.ServerId > this.timestamp[key].Item1 || (ts.ServerId == this.timestamp[key].Item1 && ts.ObjectVersion > this.timestamp[key].Item2))) {
                        serverObjects[key] = ts.ObjectValue;
                        this.timestamp[key] = new Tuple<int, int>(ts.ServerId, ts.ObjectVersion);

                    }
                }
                finally {
                   Console.WriteLine(" Unlocking ");
                   Monitor.Exit(partitionLock);
                }
            }
            Console.WriteLine("New timestamp table");
            printTimestamp();
            //Monitor.Exit(partitionLock);

            return Task.FromResult(new GossipReply {
                Ok = true
            });
        }

        public override Task<LockReply> Lock(LockRequest request, ServerCallContext context)
        {
            //Lock

            while (puppetService.frozen) ;
            System.Threading.Thread.Sleep(RandomDelay());

            Console.WriteLine("Locking server for write ");
            _semaphore.WaitOne();
            return Task.FromResult(new LockReply
            {
                Ok = true
            });
        }

        public override Task<ReadValueReply> ReadValue(ReadValueRequest request, ServerCallContext context)
        {

            while (puppetService.frozen) ;
            System.Threading.Thread.Sleep(RandomDelay());

            Console.WriteLine("Received Read request ");
            string value;
            _semaphore.WaitOne();
            Tuple<string, string> key = new Tuple<string, string>(request.PartitionId, request.ObjectId);
            if (serverObjects.ContainsKey(key))
            {
                value = serverObjects[key];
            }
            else
            {
                value = "N/A";
            }
            _semaphore.Release();
            return Task.FromResult(new ReadValueReply
            {
                Value = value
            });
        }

        public void removeCurrentMaster(String partition)
        {
            Console.WriteLine("\n\tprinting Current Masters");
            List<ServerStruct> p = puppetService.partitionServers[partition];
            for (int i=0; i < p.Count(); i++) {
                Console.WriteLine(i + " : " + p[i].url);
            }
            Console.WriteLine("Deleting");

            while (puppetService.partitionServers[partition].First().url != puppetService.url) {
                puppetService.partitionServers[partition].Remove(this.puppetService.partitionServers[partition].First());
            }


            for (int i = 0; i < p.Count(); i++) {
                Console.WriteLine(i + " : " + p[i].url);
            }
            Console.WriteLine("End Print");
        }
    }

    //Client side of the server
    //public class ServerService : GStoreServerService.GStoreServerServiceClient {
    class Program
    {

        public static void Main(string[] args)
        {
            //const int port = 1001;
            //const string hostname = "localhost";
            string startupMessage;
            ServerPort serverPort;

            String hostname = Regex.Matches(args[1], "[A-Za-z]+[^:]")[0].ToString();
            int port = int.Parse(Regex.Matches(args[1], "[^:]*[0-9]+")[0].ToString());
            int min_delay = int.Parse(args[2]);
            int max_delay = int.Parse(args[3]);
            int server_int_id = int.Parse(args[4]);

            serverPort = new ServerPort(hostname, port, ServerCredentials.Insecure);
            startupMessage = "Insecure GStoreServer server listening on port " + port;

            PuppetServerService puppetService = new PuppetServerService(hostname + ":" + port);
            ServerService serverService = new ServerService(puppetService, min_delay, max_delay, server_int_id);

            Server server = new Server
            {
                Services = { GStoreServerService.BindService(serverService),
                             PuppetMasterService.BindService(puppetService)},
                Ports = { serverPort }
            };

            server.Start();

            Console.WriteLine(startupMessage);
            Console.WriteLine("Server_id: " + args[0] + "\t hostname: " + hostname + "\t min_delay: " + min_delay + "\t max_delay: " + max_delay + "\t server_int_identifier: " + server_int_id);
            //Configuring HTTP 
            AppContext.SetSwitch(
  "System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);
            while (true)
            {
                if (puppetService.crashed)
                    break;

            }
        }
    }
}

