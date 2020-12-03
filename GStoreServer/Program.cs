using Grpc.Core;
using Grpc.Net.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf.Collections;
using System.Collections.Concurrent;
using System.Linq;

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
        public ConcurrentDictionary<String, List<ServerStruct>> partitionServers = new ConcurrentDictionary<String, List<ServerStruct>>();
        public ConcurrentDictionary<String, GStoreServerService.GStoreServerServiceClient> openChannels = new ConcurrentDictionary<String, GStoreServerService.GStoreServerServiceClient>();

        // <partition_name, integer_value> Each value is used as a lock for that partition
        public ConcurrentDictionary<string, ReaderWriterLockSlim> partitionLocks = new ConcurrentDictionary<string, ReaderWriterLockSlim>();

        //Log entrys
        public ConcurrentDictionary<String, LinkedList<Tuple<String, String>>> logEntrys = new ConcurrentDictionary<String, LinkedList<Tuple<String, String>>>();

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
                partitionServers.TryAdd(request.PartitionName, new List<ServerStruct>());
                partitionLocks.TryAdd(request.PartitionName, new ReaderWriterLockSlim());
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
                        openChannels.TryAdd(request.ServersUrls[i], service);
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
            
            logEntrys.TryAdd(request.PartitionName, new LinkedList<Tuple<string, string>>());

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
            GStoreServerService.GStoreServerServiceClient ignored;
            openChannels.TryRemove(url, out ignored);
            List<string> removals = new List<string>();
            Monitor.Enter(partitionServers);
            try {
                foreach (KeyValuePair<string, List<ServerStruct>> partition in partitionServers) {
                    if (partition.Value.Any(server => server.url == url)) {
                        removals.Add(partition.Key);
                    }
                }
                foreach (string remove in removals) {
                    partitionServers[remove] = partitionServers[remove].Where(server => server.url != url).ToList();
                }
            } finally {
                Monitor.Exit(partitionServers);
            }
        }
    }
    // GStoreServerService is the namespace defined in the protobuf
    // GStoreServerServiceBase is the generated base implementation of the service
    public class ServerService : GStoreServerService.GStoreServerServiceBase
    {
        private static int LOG_ENTRYS_LIMIT = 20;
        private static int GOSSIP_TIME = 10000;
        public PuppetServerService puppetService;

        //key: <partition_id, object_id>      value: object_value
        private ConcurrentDictionary<Tuple<String, String>, String> serverObjects = new ConcurrentDictionary<Tuple<String, String>, String>();

        //timestamp -  key: <partition_id, object_id>      value: <server_id, object_version>
        private ConcurrentDictionary<Tuple<String, String>, Tuple<int, int>> timestamp = new ConcurrentDictionary<Tuple<String, String>, Tuple<int, int>>();

        System.Timers.Timer gTimer;

        int min_delay, max_delay, server_int_id;

        private ReaderWriterLockSlim logEntryLock = new ReaderWriterLockSlim();



        public ServerService(PuppetServerService p, int min, int max, int serv_int)
        {
            puppetService = p;
            min_delay = min;
            max_delay = max;
            server_int_id = serv_int;

            gTimer = new System.Timers.Timer(GOSSIP_TIME);
            gTimer.Elapsed += sendGossipAsync;
            gTimer.AutoReset = true;
            gTimer.Enabled = true;

        }


        private int RandomDelay()
        {
            Random r = new Random();
            int delay = r.Next(min_delay, max_delay);
            //Console.WriteLine("Adding a " + delay + " ms delay to the request");
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
                if (pair.Key.Item1 == partition_id)
                    reply.ObjDesc.Add(new ObjectDescription { ObjectId = pair.Key.Item2, PartitionId = pair.Key.Item1 });
                
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
            Console.WriteLine("");
            while (puppetService.frozen) ;

            System.Threading.Thread.Sleep(RandomDelay());

            Console.WriteLine("\nReceived write request for partition " + request.PartitionId + " on objet " + request.ObjectId + " with value " + request.Value);

            //Testing if the current server is suppose to serve writes - is the leader
            string current_leader_url = null;

            //Lock list of servers for this partition
            Monitor.Enter(this.puppetService.partitionServers[request.PartitionId]);
            try {
                if (!puppetService.serverIsMaster(request.PartitionId)) {
                    Console.WriteLine("\t\t Receiving write request when this server is not the leader. Testing if other leaders are dead");

                    while (true) { //Findind current leader
                        if (this.puppetService.partitionServers.Count <= 0) {
                            Console.WriteLine("\t\t No more servers to test");
                            break;
                        }
                        var server = this.puppetService.partitionServers[request.PartitionId][0]; //gets current leader server
                        Console.WriteLine("\t\t Testing if server " + server.url + " is leader");
                        if (server.url == this.puppetService.url) {
                            Console.WriteLine("\t\t Found out I am the leader");
                            break;
                        }
                        try {
                            HeartbeatReply reply = this.puppetService.openChannels[server.url].Heartbeat(new HeartbeatRequest { }, deadline: DateTime.UtcNow.AddSeconds(5));
                            Console.WriteLine("\t\t Found out " + current_leader_url + " is the leader");
                            current_leader_url = server.url; // If answer from heartbeat is received then that server is the current leader
                            break;
                        }
                        catch (RpcException) {
                            Console.WriteLine("\t\tServer " + server.url + " is dead, removing ");
                            removeCurrentMaster(request.PartitionId);
                        }
                    }
                    
                }
            }
            finally {
                Monitor.Exit(this.puppetService.partitionServers[request.PartitionId]);
            }
            //return this message with the real leader server if this server is not the master
            if (current_leader_url != null) {
                return await Task.FromResult(new WriteValueReply {
                    Ok = false,
                    CurrentLeader = current_leader_url
                });
            }




            int newVersion = 1;
          
            List<AsyncUnaryCall<GossipReply>> pendingRequests = new List<AsyncUnaryCall<GossipReply>>();
            List<ServerStruct> serverSubset;

            ReaderWriterLockSlim partitionLock = this.puppetService.partitionLocks[request.PartitionId];
            partitionLock.EnterWriteLock();
            try {
                //Change current value
                serverObjects[new Tuple<string, string>(request.PartitionId, request.ObjectId)] = request.Value;
                Tuple<String, String> receivedObjecID = new Tuple<String, String>(request.PartitionId, request.ObjectId);


                if (timestamp.ContainsKey(receivedObjecID)) { // increment version if it already has it
                    newVersion = timestamp[receivedObjecID].Item2 + 1;
                    timestamp[receivedObjecID] = new Tuple<int, int>(server_int_id, newVersion);
                }
                else {// otherwise version = 1
                    timestamp[receivedObjecID] = new Tuple<int, int>(server_int_id, newVersion);
                }
                //Sends new write to every partition server and wait for 1 response

                Random rnd = new Random();
                //New list without this server
                IEnumerable<ServerStruct> partServers = puppetService.getPartitionServers(request.PartitionId).Where(server => server.url != puppetService.url);
                //Pick 5 random servers to send gossip
                serverSubset = partServers.OrderBy(x => rnd.Next()).Take(5).ToList();

                foreach (ServerStruct server in serverSubset) {
                    if (server.url != puppetService.url) { // dont send url to itself
                        Console.WriteLine("\t\tSending gossip with single write to " + server.url + " on object " + request.ObjectId + " with values " + request.Value);
                        try {
                            GossipRequest gossipRequest = new GossipRequest { };
                            gossipRequest.Timestamp.Add(new timestampValue {
                                PartitionId = request.PartitionId,
                                ObjectId = request.ObjectId,
                                ServerId = server_int_id,
                                ObjectVersion = newVersion,
                                ObjectValue = request.Value
                            });
                            Console.WriteLine("\t\tSending gossip " + request.PartitionId + "  " + request.ObjectId + "  " + server_int_id + "  " + newVersion + "  " + request.Value);
                            AsyncUnaryCall<GossipReply> reply = server.service.GossipAsync(gossipRequest);
                            pendingRequests.Add(reply);
                        }
                        catch (RpcException) {
                            Console.WriteLine("\t\tConnection failed to server " + server.url + " removing server");
                            puppetService.removeServer(server.url);
                        }

                    }
                }
            }
            finally {
                partitionLock.ExitWriteLock();
            }

            try {
                //wait for one response
                await Task.WhenAny(pendingRequests.Select(c => c.ResponseAsync));
                Console.WriteLine("\t\t1 response received, gossip completed ");
            }
            catch (RpcException) {
                Console.WriteLine("\t\tConnection failed to server, removing server");
                for (int i = 0; i < pendingRequests.Count(); i++) {
                    if (pendingRequests[i].ResponseAsync.Exception != null) {
                        Console.WriteLine("\t\tRemoving server: " + serverSubset[i].url); //i+1 because the master server is also in the partitionServers list
                        puppetService.removeServer(serverSubset[i].url);
                    }
                }
            }

           

            Console.WriteLine("\t\tReturning to client");
            return await Task.FromResult(new WriteValueReply
            {
                Ok = true
            });
        }


        public override Task<GossipReply> Gossip(GossipRequest request, ServerCallContext context) {

            while (puppetService.frozen) ;
            System.Threading.Thread.Sleep(RandomDelay());

            //Console.WriteLine("\nReceived Gossip " + request.Timestamp.Count());
            foreach(timestampValue ts in request.Timestamp) {
                Tuple<String, String> key = new Tuple<String, String>(ts.PartitionId, ts.ObjectId);

                //List<ServerStruct> partitionLock = this.puppetService.partitionServers[ts.PartitionId];
                //Monitor.Enter(partitionLock);
                ReaderWriterLockSlim partitionLock = this.puppetService.partitionLocks[ts.PartitionId];
                partitionLock.EnterWriteLock();
                //Console.WriteLine(" Locking ");
                try {
                    //Console.WriteLine("Reading Gossip2 with value" + ts.ObjectValue + "  " + (!this.timestamp.ContainsKey(key)));

                    // Add value to local if the local server doesn't have a timestamp for it or if received version is more recent than the current version
                    if ((!this.timestamp.ContainsKey(key)) || (ts.ServerId > this.timestamp[key].Item1 || (ts.ServerId == this.timestamp[key].Item1 && ts.ObjectVersion > this.timestamp[key].Item2))) {
                        serverObjects[key] = ts.ObjectValue;
                        this.timestamp[key] = new Tuple<int, int>(ts.ServerId, ts.ObjectVersion);
                        puppetService.logEntrys[key.Item1].AddLast(key);
                    }

                    //logEntryLock.EnterReadLock();
                    try {
                        //Send gossip if logs hit treshold
                        if (puppetService.logEntrys.Count >= LOG_ENTRYS_LIMIT) {
                            sendGossipAsync(new object(), new EventArgs());
                            resetGossipTimer();

                        }
                    }
                    finally { //logEntryLock.ExitReadLock(); }
                    }
                }
                finally {
                    partitionLock.ExitWriteLock();
                }
            }


            return Task.FromResult(new GossipReply {
                Ok = true
            });
        }


        public override Task<ReadValueReply> ReadValue(ReadValueRequest request, ServerCallContext context)
        {
            Console.WriteLine("");
            while (puppetService.frozen) ;
            System.Threading.Thread.Sleep(RandomDelay());

            Console.WriteLine("Received Read request ");
            string value;
            int server_index=-1, version=-1;
            Tuple<string, string> key = new Tuple<string, string>(request.PartitionId, request.ObjectId);

            //Lock partition
            ReaderWriterLockSlim partitionLock = this.puppetService.partitionLocks[request.PartitionId];
            partitionLock.EnterReadLock();

            if (serverObjects.ContainsKey(key)){
                value = serverObjects[key];
                server_index = timestamp[key].Item1;
                version = timestamp[key].Item2;
            } else {
                value = "N/A";
            }

            partitionLock.ExitReadLock();

            return Task.FromResult(new ReadValueReply
            {
                Value = value,
                ServerIndex = server_index,
                Version = version
            });
        }


        public async void sendGossipAsync(object o,EventArgs e)
        {
            while (puppetService.frozen) ;
            Random rnd = new Random();

            foreach (var partId in puppetService.partitionServers.Keys)
            {
                List<ServerStruct> serverSubset;
                List<AsyncUnaryCall<GossipReply>> pendingRequests;
                ReaderWriterLockSlim partitionLock = this.puppetService.partitionLocks[partId];
                partitionLock.EnterWriteLock();
                try {
                    //New list without this server
                    IEnumerable<ServerStruct> partServers = puppetService.getPartitionServers(partId).Where(server => server.url != puppetService.url);
                    //Pick 5 random servers to send gossip
                    serverSubset = partServers.OrderBy(x => rnd.Next()).Take(5).ToList();

                    pendingRequests = new List<AsyncUnaryCall<GossipReply>>();
                    foreach (ServerStruct server in serverSubset) {
                        if (server.url != puppetService.url) { // dont send url to itself
                            //Console.WriteLine("\t Sending partition " + partId + " gossip to " + server.url);
                            try {
                                GossipRequest gossipRequest = new GossipRequest { };

                                //logEntryLock.EnterReadLock();
                                try {
                                    foreach (var entry in puppetService.logEntrys[partId]) {
                                        gossipRequest.Timestamp.Add(new timestampValue {
                                            PartitionId = entry.Item1,
                                            ObjectId = entry.Item2,
                                            ServerId = timestamp[new Tuple<string, string>(entry.Item1, entry.Item2)].Item1,
                                            ObjectVersion = timestamp[new Tuple<string, string>(entry.Item1, entry.Item2)].Item2,
                                            ObjectValue = serverObjects[new Tuple<string, string>(entry.Item1, entry.Item2)]
                                        });
                                        //Console.WriteLine("\t Sending gossip " + gossipRequest);
                                    }
                                    puppetService.logEntrys[partId].Clear();
                                }
                                finally {
                                    //logEntryLock.ExitReadLock();
                                }



                                AsyncUnaryCall<GossipReply> reply = server.service.GossipAsync(gossipRequest);
                                pendingRequests.Add(reply);
                            }
                            catch (RpcException) {
                                //Console.WriteLine("Connection failed to server " + server.url + " removing server");
                                puppetService.removeServer(server.url);
                            }

                        }
                    }
                }
                finally {
                    partitionLock.ExitWriteLock();
                }

                try {
                    //wait for one response
                    await Task.WhenAny(pendingRequests.Select(c => c.ResponseAsync));

                    //Console.WriteLine("Gossip requests completed ");
                }
                catch (RpcException) {
                    //Console.WriteLine("\t\tConnection failed to server, removing server");
                    for (int i = 0; i < pendingRequests.Count(); i++) {
                        if (pendingRequests[i].ResponseAsync.Exception != null) {
                            Console.WriteLine("\t\tRemoving server: " + serverSubset[i].url); //i+1 because the master server is also in the partitionServers list
                            puppetService.removeServer(serverSubset[i].url);
                        }
                    }
                }
            }
        }

        public void resetGossipTimer()
        {
            gTimer.Stop();
            gTimer.Start();
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

