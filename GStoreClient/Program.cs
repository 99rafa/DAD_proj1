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
        public PuppetClientService(String h)
        {
            url = h;

            

        }

    }
    static class Program {
        /// <summary>
        ///  The main entry point for the application.
        /// </summary>
        [STAThread]
        static void Main(string [] args) {
            GStoreClient client;
            int i = 4;

            Console.WriteLine("Username: " + args[0] + "\t hostname: " + args[1] + "\t script_path: " + args[2] );

            String username = args[0];
            String ops_file = args[2];
            String partitions = "";

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

            partitions += args[3];
            while (args[i] != null) partitions += " " + args[i++] ;
            
            client = new GStoreClient(username, hostname, partitions);

            client.readScriptFile(ops_file);

            while (true);
        }
    }
}
