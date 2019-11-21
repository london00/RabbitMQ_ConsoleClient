using System;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ_ConsoleClient.Alternate;

namespace RabbitMQ_ConsoleClient
{
    class Program
    {


        static void Main(string[] args)
        {
            Console.WriteLine("Rabbit MQ Console Application!");
            //RabbitMQ_Fanout.RunExample();
            //RabbitMQ_Direct.RunExample();
            //RabbitMQ_Topic.RunExample();
            RabbitMQ_AlternateQueue.RunExample();
        }
    }
}
