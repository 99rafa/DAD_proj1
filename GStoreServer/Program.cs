using Grpc.Core;
using Grpc.Net.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

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
        private GrpcChannel channel;
        public PuppetServerService puppetService;

        private Dictionary<Tuple<String, String>, String> serverObjects = new Dictionary<Tuple<String, String>, String>();

        private Semaphore _semaphore = new Semaphore(1, 1);

        int min_delay, max_delay;

        public ServerService(PuppetServerService p, int min, int max)
        {
            puppetService = p;
            min_delay = min;
            max_delay = max;
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

        public async override Task<WriteValueReply> WriteValue(WriteValueRequest request, ServerCallContext context)
        {

            while (puppetService.frozen) ;
            System.Threading.Thread.Sleep(RandomDelay());

            Console.WriteLine("Received write request for partition " + request.PartitionId + " on objet " + request.ObjectId + " with value " + request.Value);
            //Check if this server is master of partition
            if (!puppetService.serverIsMaster(request.PartitionId))
            {
                Console.WriteLine("This server is not the master of partition:" + request.PartitionId);
                return await Task.FromResult(new WriteValueReply
                {
                    Ok = false
                });
            }


            _semaphore.WaitOne();
            try
            {
                serverObjects[new Tuple<string, string>(request.PartitionId, request.ObjectId)] = request.Value;

                //Sends lock request to every server in partition and wait for all acks
                List<AsyncUnaryCall<LockReply>> pendingLocks = new List<AsyncUnaryCall<LockReply>>();
                List<ServerStruct> partitionServers = puppetService.getPartitionServers(request.PartitionId);
                foreach (ServerStruct server in partitionServers)
                {
                    if (server.url != puppetService.url)
                    {
                        Console.WriteLine("\t\tSending lock request to " + server.url);
                        AsyncUnaryCall<LockReply> reply = server.service.LockAsync(new LockRequest { });
                        pendingLocks.Add(reply);
                        
                    }

                }

                try {
                    //wait for all LOCK responses
                    await Task.WhenAll(pendingLocks.Select(c => c.ResponseAsync));
                    Console.WriteLine("\tLock requests completed");
                }
                catch (RpcException e) {
                    Console.WriteLine("\t\tConnection failed to server, removing server");
                    for(int i=0; i<pendingLocks.Count(); i++) {
                        if(pendingLocks[i].ResponseAsync.Exception != null) {
                            Console.WriteLine("\t\tRemoving server: " + partitionServers[i+1].url); //i+1 because the master server is also in the partitionServers list
                            puppetService.removeServer(partitionServers[i + 1].url);
                        }
                    }
                }

            //Share write with all replicas
            List<AsyncUnaryCall<ShareWriteReply>> pendingTasks = new List<AsyncUnaryCall<ShareWriteReply>>();
                foreach (ServerStruct server in puppetService.getPartitionServers(request.PartitionId))
                {
                    if (server.url != puppetService.url)
                    {
                        Console.WriteLine("\t\tSending write share request to " + server.url);
                        try
                        {
                            AsyncUnaryCall<ShareWriteReply> reply = server.service.ShareWriteAsync(new ShareWriteRequest
                            {
                                PartitionId = request.PartitionId,
                                ObjectId = request.ObjectId,
                                Value = request.Value
                            });
                            pendingTasks.Add(reply);
                        }
                        catch (RpcException)
                        {
                            Console.WriteLine("Connection failed to server " + server.url + " removing server");
                            puppetService.removeServer(server.url);
                        }
                    }
                }
                try { 
                    //wait for all WRITE SHARE responses
                    await Task.WhenAll(pendingTasks.Select(c => c.ResponseAsync));
                    Console.WriteLine("\tSharing writes completed");
                }
                catch (RpcException e) {
                    Console.WriteLine("\t\tConnection failed to server, removing server");
                    for (int i = 0; i < pendingLocks.Count(); i++) {
                        if (pendingLocks[i].ResponseAsync.Exception != null) {
                            Console.WriteLine("\t\tRemoving server: " + partitionServers[i + 1].url); //i+1 because the master server is also in the partitionServers list
                            puppetService.removeServer(partitionServers[i + 1].url);
                        }
                    }
                }
                Console.WriteLine("Write in partition completed");
            }
            finally
            {
                _semaphore.Release();
            }

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

            serverPort = new ServerPort(hostname, port, ServerCredentials.Insecure);
            startupMessage = "Insecure GStoreServer server listening on port " + port;

            PuppetServerService puppetService = new PuppetServerService(hostname + ":" + port);
            ServerService serverService = new ServerService(puppetService, min_delay, max_delay);

            Server server = new Server
            {
                Services = { GStoreServerService.BindService(serverService),
                             PuppetMasterService.BindService(puppetService)},
                Ports = { serverPort }
            };

            server.Start();

            Console.WriteLine(startupMessage);
            Console.WriteLine("Server_id: " + args[0] + "\t hostname: " + hostname + "\t min_delay: " + min_delay + "\t max_delay: " + max_delay);
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

