﻿using Grpc.Core;
using Grpc.Net.Client;
using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Data;
using System.Drawing;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

public delegate void DelAddMsg(string s);

namespace GStoreClient {

    struct ClientStruct
    {
        public string url;
        public GStoreServerService.GStoreServerServiceClient service;

        public ClientStruct(string u, GStoreServerService.GStoreServerServiceClient s)
        {
            url = u;
            service = s;
        }
    }
    public interface IGStoreClientService {
        bool AddMsgtoGUI(string s);
    }
    public class GStoreClient : IGStoreClientService {
        private  GrpcChannel channel;
        Queue<String> commandQueue = new Queue<String>();
        private GStoreServerService.GStoreServerServiceClient current_server;
        private string username;
        private string hostname;
        // dictionary with serverID as key and clientStruct
        private Dictionary<string, ClientStruct> serverMap =
            new Dictionary<string,ClientStruct>();
        // dictionary with partitionID as key and list of serverID's
        private Dictionary<string, List<string>> partitionMap = new Dictionary<string, List<string>>();

        public GStoreClient(String user, String host, String args ) {
            username = user;
            hostname = host;

            //partitions come in command line format: -p partition_id partition_master_id partition_master_url other_servers_id other_servers_url -p 
            //maybe it should not be here as it is command line logic
            
            String[] partitions = args.Split("-p ");
            String[] fields;
            String partition_id;
            List<string> partitionServers = new List<string>();

            foreach ( var partition in partitions)
            {
                fields = partition.Split(" ");
  
                partition_id = fields[0];
                
                for (int i = 1; i < fields.Length; i+=2)
                {
                    partitionServers.Add(fields[i]);
                    AddServerToDict(fields[i], fields[i+1]);
                }
                AddPartitionToDict(partition_id, partitionServers);
                
                Array.Clear(fields, 0, fields.Length);
                partitionServers.Clear();
            }
        }

        public bool AddMsgtoGUI(string s) {
            return true;
        }

        private void AddServerToDict(String server_id, String url)
        {
            Console.WriteLine("server_id: " + server_id + " url: " + url);
            GrpcChannel channel = GrpcChannel.ForAddress("http://" + url);
            GStoreServerService.GStoreServerServiceClient client = new GStoreServerService.GStoreServerServiceClient(channel);
            ClientStruct server = new ClientStruct(url, client);
            serverMap.Add(server_id, server);
        }


        private void  AddPartitionToDict(String id, List<string> servers)
        {
            partitionMap.Add(id, servers);
        }


        public String ReadValue(
           string partitionId, string objectId, string serverId)
        {
            AppContext.SetSwitch(
                      "System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);

            ReadValueReply reply = current_server.ReadValue(new ReadValueRequest
            {
                PartitionId = partitionId,
                ObjectId = objectId,
            });
            if (reply.Value.Equals("N/A"))
            {
                AppContext.SetSwitch(
                        "System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);

                GStoreServerService.GStoreServerServiceClient new_server = serverMap[serverId].service;

                reply = new_server.ReadValue(new ReadValueRequest
                {
                    PartitionId = partitionId,
                    ObjectId = objectId,
                });
                current_server = new_server;
            }
            return reply.Value;
        }

        public bool WriteValue(
           string partitionId, string objectId, string value)
        {

            AppContext.SetSwitch(
                    "System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);

            //Assuming the replica master is the first element of the list - 
            string serverID = partitionMap[partitionId].ElementAt(0);

            GStoreServerService.GStoreServerServiceClient client = serverMap[serverID].service;

            WriteValueReply reply = client.WriteValue(new WriteValueRequest
            {
                PartitionId = partitionId,
                ObjectId = objectId,
                Value = value
            });

            return reply.Ok;
        }

        public void readScriptFile(String file)
        {
            String line;
            if (!File.Exists(file))
            {


                //TODO
            }
            else
            {
                System.IO.StreamReader fileStream = new System.IO.StreamReader(file);

                while ((line = fileStream.ReadLine()) != null)
                {
                    addComand(line);
                }

                fileStream.Close();

                processCommands();
            }
        }

        public bool isEnd(String command){
            string[] args = command.Split(" ");
            if (args[0] == "end-repeat")
                return true;
            return false;
        }

        public bool isBegin(String command)
        {
            string[] args = command.Split(" ");
            if (args[0] == "begin-repeat")
                return true;
            return false;
        }
        public void beginRepeat(int x,int line){
            List<String> block = new List<String>();
            int begin = 0;
            int end = 0;

            int i = 1;
            int c = 1;
            foreach(var command in commandQueue)
            {
                if(c > line && begin == end)
                {
                    command.Replace("$i", i.ToString());
                    if (isBegin(command))
                    {
                        begin++;
                    }
                    if (isEnd(command))
                    {
                        end++;
                        break;
                    }
                    runOperation(command,line+i);
                    i++;
                }
                c++;
            }

        }

        public void processCommands()
        {
            int line = 1;
            foreach (var command in commandQueue)
            {
                runOperation(command,line);
                line++;
            }
            commandQueue.Clear();
        }

        public void runOperation(string op,int line)
        {
            string[] args = op.Split(" ");
            switch (args[0])
            {
                case "read":
                    String partition_id = args[1];
                    String object_id = args[2];
                    String server_id = args[3];
                    ReadValue(partition_id, object_id, server_id);
                    break;
                case "write":
                    break;
                case "ListServer":
                    Console.WriteLine("List Server instruction");
                    break;
                case "ListGlobal":
                    Console.WriteLine("ListGlobal instruction");
                    break;
                case "wait":
                    Console.WriteLine("Wait instruction");
                    break;
                case "begin-repeat":
                    String x = args[1];
                    beginRepeat(int.Parse(x),line);
                    break;
                case "end-repeat":
                    break;
                default:
                    break;
            }
        }

        public void addComand(String command)
        {
            commandQueue.Enqueue(command);
            System.Diagnostics.Debug.WriteLine("added command:", command);
        }

        public void ServerShutdown(Server server) {
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
