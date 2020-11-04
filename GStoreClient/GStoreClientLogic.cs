using Grpc.Core;
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
  
            String[] partitions = args.Split("-p ", StringSplitOptions.RemoveEmptyEntries);
            Console.WriteLine(args);
            foreach ( var partition in partitions)
            {
                AddPartitionToDict(partition);
            }
        }

        public bool AddMsgtoGUI(string s) {
            return true;
        }

        private void AddServerToDict(String server_id, String url)
        {

            GrpcChannel channel = GrpcChannel.ForAddress("http://" + url);
            GStoreServerService.GStoreServerServiceClient client = new GStoreServerService.GStoreServerServiceClient(channel);
            ClientStruct server = new ClientStruct(url, client);
            serverMap.Add(server_id, server);
        }

        // receives arguments in the format: partition_master_id partition_master_url server2_id server2_url ....
        private void  AddPartitionToDict(String servers)
        {
            String [] fields = servers.Split(" ", StringSplitOptions.RemoveEmptyEntries);
            String partition_id = fields[0];

            partitionMap.Add(partition_id, new List<string>());
            for (int i = 1; i < fields.Length; i += 2)
            {
                String server_id = fields[i];
                String server_url = fields[i + 1];

                partitionMap[partition_id].Add(server_id);
                if(!serverMap.ContainsKey(server_id))
                    AddServerToDict(server_id, server_url);
            }            

        }


        public String ReadValue(
           string partition_id, string object_id, string server_id)
        {
            if (current_server == null) current_server = serverMap[server_id].service;

            Console.WriteLine("Connecting to current server...");

            if (!partitionMap.ContainsKey(partition_id)) return "N/A";

            ReadValueReply reply = current_server.ReadValue(new ReadValueRequest
            {
                PartitionId = partition_id,
                ObjectId = object_id,
            });
            if (reply.Value.Equals("N/A"))
            {
                Console.WriteLine("Unable to fetch object " + object_id + " from current server");
                Console.WriteLine("Trying server " + server_id + "...");
                GStoreServerService.GStoreServerServiceClient new_server = serverMap[server_id].service;

                reply = new_server.ReadValue(new ReadValueRequest
                {
                    PartitionId = partition_id,
                    ObjectId = object_id,
                });
                current_server = new_server;
            }
            return reply.Value;
        }

        public bool WriteValue(
           string partition_id, string object_id, string value)
        {
           //Assuming the replica master is the first element of the list  
            string server_id = partitionMap[partition_id].First();

            GStoreServerService.GStoreServerServiceClient master = serverMap[server_id].service;
            current_server = master;

            Console.WriteLine("Connecting to master replica with server_id " + server_id + " of partition " + partition_id);
            Console.WriteLine("Sending Write to partition " + partition_id + " on object " + object_id + " with value " + value);

            WriteValueReply reply = master.WriteValue(new WriteValueRequest
            {
                PartitionId = partition_id,
                ObjectId = object_id,
                Value = value
            }) ;
            return reply.Ok;
        }

        public void ListServer( String server_id)
        {
            GStoreServerService.GStoreServerServiceClient server = serverMap[server_id].service;

            ListServerObjectsReply reply = server.ListServerObjects(new ListServerObjectsRequest { });

            Console.WriteLine("Server " + server_id + " stores the following objects:");
            foreach ( var obj in reply.Objects) {
                if (obj.IsMaster)
                    Console.WriteLine("Object " + obj.ObjectId + " with value " + obj.Value + " in partition " + obj.PartitionId + "(master for this partition).");
                else
                    Console.WriteLine("Object " + obj.ObjectId + " with value " + obj.Value + " in partition " + obj.PartitionId);
            }
        }

        public void ListGlobal()
        {
            List<String> masters = new List<String>();
            foreach(var pair in partitionMap)
            {
                String master = pair.Value[0];

                if (!masters.Contains(master))
                    masters.Add(master);

            }

            foreach(String master in masters)
            {
                GStoreServerService.GStoreServerServiceClient server = serverMap[master].service;

                ListGlobalReply reply = server.ListGlobal(new ListGlobalRequest { });
                foreach(var obj in reply.ObjDesc)
                    Console.WriteLine("Partition_id: " + obj.PartitionId +" , Object_id: " + obj.ObjectId);
            }


        }

        public void readScriptFile(String file)
        {
            Console.WriteLine("File:" + file);
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
            return command.Split(" ")[0] == "end-repeat";
        }

        public bool isBegin(String command)
        {
            return command.Split(" ")[0] == "begin-repeat";
        }
        public void beginRepeat(int x,int line){
            List<String> block = new List<String>();

            //Repeating x times
            for (int i = 1; i <= x; i++)
            {
                int current_line = 1;
                int executed_commands = 1;
                int context = 0;

                //
                foreach (var command in commandQueue)
                {
                    if (current_line > line && isEnd(command))
                    {
                        //Reached current context end-repeat
                        if (context == 0)
                            break;

                        context--;  //Change context
                        executed_commands++;
                    }
                    //Only exec commands that come after called begin and on current context
                    if (current_line > line && context == 0)
                    {
                        //$i --> current iteration
                        String rcommand = command.Replace("$i", i.ToString());

                        //Change context
                        if (isBegin(command))
                            context++;

                        runOperation(rcommand, line + executed_commands);
                        executed_commands++;
                    }
                    current_line++;
                }
            }

        }


        public int updateContext(String command)
        {
            if (isEnd(command))
                return -1;
            else if (isBegin(command))
                return 1;
            else
                return 0;
        }

        public bool isCorrectRepeat()
        {
            int begin = 0, end = 0;
            foreach (var command in commandQueue)
            {
                if (isBegin(command)) begin++;

                else if (isEnd(command)) end++;

                if (end > begin) return false;
            }

            return begin == end;
        }
        public void processCommands()
        {
            int line = 1;
            int context = 0;

            if (!isCorrectRepeat())
            { 
                Console.Error.WriteLine("Syntax Error: Begin/End-repeat loop");
                commandQueue.Clear();
            } 
            foreach (var command in commandQueue)
            {
                if(context == 0)
                    runOperation(command,line);
                context += updateContext(command);
                line++;
            }
            commandQueue.Clear();
        }

        public void runOperation(string op,int line)
        {
            String partition_id, object_id, server_id;
            string[] args = op.Split(" ");
            switch (args[0])
            {
                case "read":
                    partition_id = args[1];
                    object_id = args[2];
                    server_id = args[3];
                    String read_value = ReadValue(partition_id, object_id, server_id);
                    Console.WriteLine("Read value " + read_value + " on partition " + partition_id + " on object " + object_id);
                    break;
                case "write":
                    partition_id = args[1];
                    object_id = args[2];
                    String value = args[3];
                    WriteValue(partition_id, object_id, value);
                    break;
                case "listServer":
                    server_id = args[1];
                    Console.WriteLine("Reading all objects from server " + server_id);
                    ListServer(server_id);
                    break;
                case "listGlobal":
                    Console.WriteLine("ListGlobal instruction");
                    ListGlobal();
                    break;
                case "wait":
                    String ms = args[1];
                    Console.WriteLine("Delaying execution for " + ms + " milliseconds");
                    System.Threading.Thread.Sleep(int.Parse(ms));
                    Console.WriteLine("Program resumed");
                    break;
                case "begin-repeat":                                                                                                                                                                                                                                                                               
                    beginRepeat(int.Parse(args[1]), line);
                    break;
                case "end-repeat":
                    break;
                default:
                    Console.Error.WriteLine("Error: Not a recognized operation");
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
