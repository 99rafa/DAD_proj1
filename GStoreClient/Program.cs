using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace GStoreClient
{
    static class Program {
        /// <summary>
        ///  The main entry point for the application.
        /// </summary>
        [STAThread]
        static void Main(string [] args) {
            
            Console.WriteLine("Username: " + args[0] + "\t hostname: " + args[1] + "\t script_path: " + args[2] );

            
            while (true);
        }
    }
}
