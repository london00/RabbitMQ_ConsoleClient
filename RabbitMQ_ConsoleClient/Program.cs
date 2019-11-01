using System;

namespace RabbitMQ_ConsoleClient
{
    class Program
    {


        static void Main(string[] args)
        {
            Console.WriteLine("Rabbit MQ Console Application!");

            using (RabbitMQHelper rabbitMQHelper = new RabbitMQHelper())
            {
                rabbitMQHelper.PublishMessage("Hi there");
                rabbitMQHelper.PublishMessage("How are you?");
            }
        }
    }
}
