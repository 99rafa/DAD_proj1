using Grpc.Core;
using Grpc.Net.Client;
using System;
using System.Collections;
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
        private  GrpcChannel channel;
        private Server server;
        private string nick;
        private string hostname;
        private AsyncUnaryCall<BcastMsgReply> lastMsgCall;
        private Dictionary<string, GStoreServerService.GStoreServerServiceClient> serverMap =
            new Dictionary<string, GStoreServerService.GStoreServerServiceClient>();
        private Dictionary<string, ArrayList> partitionMap = new Dictionary<string, ArrayList>();

        public GStoreClient() {
   
           
        }

        public bool AddMsgtoGUI(string s) {
            return true;
        }

        private void addServerToDict(String server_id, String url)
        {
            GrpcChannel channel = GrpcChannel.ForAddress("http://" + url);
            GStoreServerService.GStoreServerServiceClient client = new GStoreServerService.GStoreServerServiceClient(channel);
            serverMap.Add(server_id, client);
        }


        private void  addPartitionToDict(String id, ArrayList servers)
        {
            partitionMap.Add(id, servers);
        }


        public String ReadValue(
           string partitionId, string objectId, string serverId)
        {

            AppContext.SetSwitch(
                    "System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);

            GStoreServerService.GStoreServerServiceClient client = serverMap[serverId];

            ReadValueReply reply = client.ReadValue(new ReadValueRequest
            {
               PartitionId = partitionId,
               ObjectId = objectId,
               ServerId = serverId
            });

            return reply.Value;
        }

        public bool WriteValue(
           string partitionId, string objectId, string value)
        {

            AppContext.SetSwitch(
                    "System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);

            GStoreServerService.GStoreServerServiceClient client = serverMap[partitionMap[partitionId].IndexOf(0)];

            WriteValueReply reply = client.WriteValue(new WriteValueRequest
            {
                PartitionId = partitionId,
                ObjectId = objectId,
                Value = value
            });

            return reply.Ok;
        }

        public void ServerShutdown() {
            server.ShutdownAsync().Wait();
        }
    }


    public class ClientService : GStoreClientService.GStoreClientServiceBase
    {
        IGStoreClientService clientLogic;
        

        public ClientService(IGStoreClientService clientLogic)
        {
            this.clientLogic = clientLogic;
        }

        public override Task<RecvMsgReply> RecvMsg(
            RecvMsgRequest request, ServerCallContext context)
        {
            return Task.FromResult(UpdateGUIwithMsg(request));
        }

        public RecvMsgReply UpdateGUIwithMsg(RecvMsgRequest request)
        {
            if (clientLogic.AddMsgtoGUI(request.Msg))
            {
                return new RecvMsgReply
                {
                    Ok = true
                };
            }
            else
            {
                return new RecvMsgReply
                {
                    Ok = false
                };

            }
        }

    }
}
