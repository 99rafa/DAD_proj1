using Grpc.Core;
using Grpc.Net.Client;
using System;
using System.Collections.Generic;
using System.Drawing;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

public delegate void DelAddMsg(string s);

namespace GStoreClient {
    public interface IGStoreClientService {
        bool AddMsgtoGUI(string s);
    }
    public class GStoreClient : IGStoreClientService {
        private readonly GrpcChannel channel;
        private readonly GStoreServerService.GStoreServerServiceClient client;
        private Server server;
        private string nick;
        private string hostname;
        private AsyncUnaryCall<BcastMsgReply> lastMsgCall;

        public GStoreClient(bool sec, string serverHostname, int serverPort, 
                             string clientHostname) {
            this.hostname = clientHostname;
            // setup the client side

                AppContext.SetSwitch(
                    "System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);
                channel = GrpcChannel.ForAddress("http://" + serverHostname + ":" + serverPort.ToString());
   
            client = new GStoreServerService.GStoreServerServiceClient(channel);
        }

        public bool AddMsgtoGUI(string s) {
            return true;
        }

        public List<string> Register(string nick, string port) {
            this.nick = nick;
            // setup the client service
            server = new Server
            {
                Services = { GStoreClientService.BindService(new ClientService(this)) },
                Ports = { new ServerPort(hostname, Int32.Parse(port), ServerCredentials.Insecure) }
            };
            server.Start();
            GStoreClientRegisterReply reply = client.Register(new GStoreClientRegisterRequest
            {
                Nick = nick,
                Url = "http://localhost:" + port
            }) ;
            
            List<string> result = new List<string>();
            foreach (User u in reply.Users) {
                result.Add(u.Nick);
            }
            return result;
        }

        public async Task BcastMsg(string m) {
            BcastMsgReply reply;
            if (lastMsgCall != null) {
                reply = await lastMsgCall.ResponseAsync;          
            }
            lastMsgCall = client.BcastMsgAsync(new BcastMsgRequest { 
                Nick = this.nick,
                Msg = m
            });
        }

        public void ServerShutdown() {
            server.ShutdownAsync().Wait();
        }
    }


    public class ClientService : GStoreClientService.GStoreClientServiceBase {
        IGStoreClientService clientLogic;

        public ClientService(IGStoreClientService clientLogic) {
            this.clientLogic = clientLogic;
        }

        public override Task<RecvMsgReply> RecvMsg(
            RecvMsgRequest request, ServerCallContext context) {
            return Task.FromResult(UpdateGUIwithMsg(request));
        }

        public RecvMsgReply UpdateGUIwithMsg(RecvMsgRequest request) {
           if ( clientLogic.AddMsgtoGUI(request.Msg)) { 
            return new RecvMsgReply
            {
                Ok = true
            };
            } else {
                return new RecvMsgReply
                {
                    Ok = false
                };

            }
        }
    }
}
