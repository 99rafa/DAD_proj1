using Grpc.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace GStoreClient

{
    public class PuppetClientService : PuppetMasterService.PuppetMasterServiceBase
    {
        public String url;
        //List of servers in each partition
        public Dictionary<String, List<String>> partitionServers = new Dictionary<String, List<String>>();
        //List of master for each partition
        public Dictionary<String, String> partitionMaster = new Dictionary<string, string>(); 
        public PuppetClientService(String h)
        {
            url = h;
        }
        public Dictionary<String, List<String>> getPartitions()
        {
            return partitionServers;
        }
        public Dictionary<String, String> getPartitionsMaster()
        {
            return partitionMaster;
        }

    }
    static class Program {
        /// <summary>
        ///  The main entry point for the application.
        /// </summary>
        [STAThread]
        static void Main(string [] args) {
            GStoreClient client;

            Console.WriteLine("Username: " + args[0] + "\t hostname: " + args[1] + "\t script_path: " + args[2] );

            String username = args[0];
            String ops_file = args[2];

            String hostname = Regex.Matches(args[1], "[A-Za-z]+[^:]")[0].ToString();
            int port = int.Parse(Regex.Matches(args[1], "[^:]*[0-9]+")[0].ToString());

            ServerPort serverPort = new ServerPort(hostname, port, ServerCredentials.Insecure);

            Server server = new Server
            {
                Services = { PuppetMasterService.BindService(new PuppetClientService(hostname + ":" + port)) },
                Ports = {serverPort}
            };
            server.Start();

            AppContext.SetSwitch(
                    "System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);

            //TODO: add partitions and servers to client
            client = new GStoreClient(username, hostname);

            client.readScriptFile(ops_file);

            while (true);
        }
    }
}
