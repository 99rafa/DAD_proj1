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
    // GStoreServerService is the namespace defined in the protobuf
    // GStoreServerServiceBase is the generated base implementation of the service
    public class ServerService : GStoreServerService.GStoreServerServiceBase {
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
                Services = { GStoreServerService.BindService(new ServerService()) },
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

