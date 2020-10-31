using Grpc.Core;
using Grpc.Net.Client;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;
using System.IO;
using System.Reflection;

namespace PuppetMaster {
    struct ServerStruct {
        public string url;
        public PuppetMasterService.PuppetMasterServiceClient service;

        public ServerStruct(string u, PuppetMasterService.PuppetMasterServiceClient s) {
            url = u;
            service = s;
        }
    }

    class PuppetMaster {
        public const String hostname = "localhost";
        public const String port = "10001";
        
        private Server server;


        Queue<String> commandQueue= new Queue<String>();
        List<GrpcChannel> channels = new List<GrpcChannel>();


        Dictionary<String, ServerStruct> servers = new Dictionary<String, ServerStruct>();
        // key is partition_id and value is server_id
        Dictionary<String, List<String>> partitions = new Dictionary<String, List<String>>();

        Dictionary<String, PuppetMasterService.PuppetMasterServiceClient> clients = new Dictionary<String, PuppetMasterService.PuppetMasterServiceClient>();
  
        public PuppetMaster() {
            // setup the puppet master service
            
            server = new Server {
                Services = { PuppetMasterService.BindService(new PuppetService()) },
                Ports = { new ServerPort(hostname, Int32.Parse(port), ServerCredentials.Insecure) }
            };
            server.Start();

            AppContext.SetSwitch(
                "System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);

            
        }

        public void addComand(String command) {
            commandQueue.Enqueue(command);
            System.Diagnostics.Debug.WriteLine("added command:", command);
        }

        public void runCommands() {
            foreach(var command in commandQueue) {
                executeCommand(command);
                System.Diagnostics.Debug.WriteLine("executing command:",command);
            }
            commandQueue.Clear();
        }

        public void runNextCommand(){
            executeCommand(commandQueue.Dequeue());
        }

        private PuppetMasterService.PuppetMasterServiceClient createClientService(String url) {
            GrpcChannel channel = GrpcChannel.ForAddress("http://" + url);
            return new PuppetMasterService.PuppetMasterServiceClient(channel);
        }
        private void addServerToDict(String server_id, String url){
            ServerStruct server = new ServerStruct(url, createClientService(url));
            servers.Add(server_id, server);
        }
        private void addClientToDict(String username, String url)
        {
            PuppetMasterService.PuppetMasterServiceClient clientService = createClientService(url);
            clients.Add(username, createClientService(url));
        }
        private void addServerToPartition(String partition_id, String server_id)
        {
            if (!partitions.ContainsKey(partition_id)) partitions.Add(partition_id, new List<string>());
            partitions[partition_id].Add(server_id);
        }

        public void Status()
        {

            foreach (var server in servers)
            {
                _ = server.Value.service.StatusAsync(new StatusRequest { });
            }
            foreach (var client in clients)
            {
                _ = client.Value.StatusAsync(new StatusRequest { });
            }

        }

        public String buildServersArguments()
        {
            String args = "";
            foreach (KeyValuePair<string, List<String>> partition in partitions)
            {
                args += " -p " + partition.Key ;
                foreach (var server in partition.Value)
                    args += " " + server + " " + servers[server].url;

            }

            return args;
        }

        public void executeCommand(String c) {
            string[] args = c.Split(" ");
            String server_id;
            switch (args[0]) {
                case "ReplicationFactor":
                    break;
                case "Server":
                    server_id = args[1];
                    String url = args[2];   //ex: localhost:1001
                    String min_delay = args[3];
                    String max_delay = args[4];
                    System.Diagnostics.Debug.WriteLine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location));
                   
                    Process process = new Process();
                    if(!servers.ContainsKey(server_id))
                        addServerToDict(server_id, url);
                    //Path to server .exe , maybe it should be the "release" version instead of "debug"
                    process.StartInfo.FileName = "..\\..\\..\\..\\GStoreServer\\bin\\Debug\\netcoreapp3.1\\GStoreServer.exe";
                    process.StartInfo.Arguments = server_id + " " + url + " " + min_delay + " " + max_delay;
                    process.Start();
                    break;
                case "Partition":
                    int r = int.Parse(args[1]);
                    String part_id = args[2];
                    List<String> server_urls= new List<String>();
                    for (int i = 0; i < r; i++) {
                        server_urls.Add(servers[args[i+3]].url);
                        
                    }                    

                    for (int i = 0; i < r; i++) {
                        server_id = args[i + 3];
                        addServerToDict(part_id, server_id);
                        PartitionReply reply = servers[server_id].service.Partition(new PartitionRequest {
                            PartitionId = part_id,
                            ServersUrls = { server_urls }
                        });
                       
                        System.Diagnostics.Debug.WriteLine("Received answer from partition: " + reply.Ok);
                        
                    }
                    break;
                case "Client":
                    String username = args[1];
                    String client_url = args[2];
                    String script_file = args[3];
                    System.Diagnostics.Debug.WriteLine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location));

                    Process client_process = new Process();
                    if (!clients.ContainsKey(username))
                        addClientToDict(username,client_url);
                    client_process.StartInfo.FileName = "..\\..\\..\\..\\GStoreClient\\bin\\Debug\\netcoreapp3.1\\GStoreClient.exe";

                    //-p defines a new partion: first argument after is partition_id, next are partitionMaster, server , server....."
                    String serversArgs = buildServersArguments();
                    client_process.StartInfo.Arguments = username + " " + client_url + " " + script_file + serversArgs  ;
                    client_process.Start(); 
                    break;
                case "Status":
                    Status();
                    break;
                case "Crash":
                    break;
                case "Freeze":
                    break;
                case "Unfreeze":
                    break;
                case "Wait":
                    string ms = args[1];
                    System.Threading.Thread.Sleep(int.Parse(ms));
                    break;
                default:
                    break;
            }

        }
    }

    public class PuppetService : PuppetMasterService.PuppetMasterServiceBase {
        public PuppetService() {

        }

        //TODO
        public override Task<ReplicationFactorReply> ReplicationFactor(ReplicationFactorRequest request, ServerCallContext context) {
            return Task.FromResult(new ReplicationFactorReply { }) ;
        }
       
    }
}
