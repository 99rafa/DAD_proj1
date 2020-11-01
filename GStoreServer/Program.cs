using Grpc.Core;
using Grpc.Net.Client;
using System;
using System.Collections.Generic;
using System.IO;
using System.Security;
using System.Threading;
using System.Threading.Tasks;
using System.Text.RegularExpressions;

namespace gStoreServer {

    public class PuppetServerService : PuppetMasterService.PuppetMasterServiceBase {
        public String url;
        //List of servers in each partition, if an entry for a partition exists then this server belongs to that partition
        public Dictionary<String, List<String>> partitionServers = new Dictionary<String, List<String>>();
        //List of replica names where this server is the master replica
        List<String> replicaMaster = new List<String>();
        public PuppetServerService(String h) {
            url = h;
        }

        public override Task<PartitionReply> Partition(PartitionRequest request, ServerCallContext context) {
            Console.WriteLine("Received partition request: partition_name: " + request.PartitionName);
            if (!partitionServers.ContainsKey(request.PartitionName)) {
                partitionServers.Add(request.PartitionName, new List<String>());
            } else {
                Console.WriteLine("Received partition creation request that already exists: " + request.PartitionName);
            }
            for(int i = 0; i < request.ServersUrls.Count; i++) {
                //Add to replicaMaster if this server is the master
                if (request.ServersUrls[i].Equals(url)) {
                    if (i == 0) {
                        Console.WriteLine("\tThis Server: " + url + "  is master of partition: " + request.PartitionName);
                        replicaMaster.Add(request.PartitionName); 
                    }
                } else {//else add server url to the list of servers from this partition
                    Console.WriteLine("\tAdded server " + request.ServersUrls[i] +  " to local list of partition servers: " + request.PartitionName);
                    partitionServers[request.PartitionName].Add(request.ServersUrls[i]);
                }
            }
            return Task.FromResult(new PartitionReply {
                Ok = true
            });

        }

        public override Task<StatusReply> Status(StatusRequest request, ServerCallContext context)
        {

            Console.WriteLine("Status request received!!");

            return Task.FromResult(new StatusReply
            {
                Ok = true
            });

        }

        public override Task<CrashReply> Crash(CrashRequest request, ServerCallContext context)
        {

            Console.WriteLine("Crash request received!!");

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

        public ServerService() {
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

            Server server = new Server
            {
                Services = { GStoreServerService.BindService(new ServerService()),
                             PuppetMasterService.BindService(new PuppetServerService(hostname + ":" + port))},
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

