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
        Dictionary<String, String> servers = new Dictionary<String, String>();

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
            switch (args[0]) {
                case "ReplicationFactor":
                    break;
                case "Server":
                    String server_id = args[1];
                    String url = args[2];   //ex: localhost:1001
                    String min_delay = args[3];
                    String max_delay = args[4];
                    System.Diagnostics.Debug.WriteLine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location));
                   
                    Process process = new Process();
                    if(!servers.ContainsKey(server_id))
                        servers.Add(server_id, url);
                    //Path to server .exe , maybe it should be the "release" version instead of "debug"
                    process.StartInfo.FileName = "..\\..\\..\\..\\GStoreServer\\bin\\Debug\\netcoreapp3.1\\GStoreServer.exe";
                    process.StartInfo.Arguments = server_id + " " + url + " " + min_delay + " " + max_delay;
                    process.Start();
                    break;
                case "Partition":
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
