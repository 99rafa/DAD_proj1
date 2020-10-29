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
    class PuppetMaster {
        public const String hostname = "localhost";
        public const String port = "10001";
        private readonly GrpcChannel channel;
        private readonly PuppetMasterService.PuppetMasterServiceClient client;
        private Server server;

        List<String> commandQueue= new List<String>();
        Dictionary<String, String> servers_url = new Dictionary<String, String>();

        public PuppetMaster() {
            // setup the puppet master service
            server = new Server {
                Services = { PuppetMasterService.BindService(new PuppetService()) },
                Ports = { new ServerPort(hostname, Int32.Parse(port), ServerCredentials.Insecure) }
            };
            server.Start();

            AppContext.SetSwitch(
                "System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);
            //channel = GrpcChannel.ForAddress("http://" + serverHostname + ":" + serverPort);
            //client = new PuppetMasterService.PuppetMasterServiceClient(channel);

            
        }

        public void addComand(String command) {
            commandQueue.Add(command);
            System.Diagnostics.Debug.WriteLine("added command:", command);
        }

        public void runCommands() {
            foreach(var command in commandQueue) {
                executeCommand(command);
                System.Diagnostics.Debug.WriteLine("executing command:",command);
            }
            commandQueue.Clear();
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
                    if(!servers_url.ContainsKey(server_id))
                        servers_url.Add(server_id, url);
                    //Path to server .exe , maybe it should be the "release" version instead of "debug"
                    process.StartInfo.FileName = "..\\..\\..\\..\\GStoreServer\\bin\\Debug\\netcoreapp3.1\\GStoreServer.exe";
                    process.StartInfo.Arguments = server_id + " " + url + " " + min_delay + " " + max_delay;
                    process.Start();
                    break;
                case "Partition":
                    int r = int.Parse(args[1]);
                    String part_name = args[2];
                    for (int i = 0; i < r; i++) {
                        server_id = args[i + 3];
                        AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);
                        GrpcChannel channel = GrpcChannel.ForAddress("http://" + servers_url[server_id]);
                        PuppetMasterService.PuppetMasterServiceClient client = new PuppetMasterService.PuppetMasterServiceClient(channel);
                        PartitionReply reply = client.Partition(new PartitionRequest { 
                            PartitionName = part_name
                        });
                        if (reply.Ok == true) {
                            System.Diagnostics.Debug.WriteLine("Received answer from partition: " + reply.Ok);
                        } else {
                            System.Diagnostics.Debug.WriteLine("Received answer from partition f: " + reply.Ok);
                        }
                    }
                    break;
                case "Client":
                    break;
                case "Status":
                    break;
                case "Crash":
                    break;
                case "Freeze":
                    break;
                case "Unfreeze":
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
