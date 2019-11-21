using RabbitMQ.Client;
using RabbitMQ_ConsoleClient.Base;
using System;
using System.Collections.Generic;

namespace RabbitMQ_ConsoleClient.Alternate
{
    public class RabbitMQ_AlternateQueue : RabbitMQ_Base, IDisposable
    {
        private const string QUEUE_NAME_1 = "my.queue.1";
        private const string QUEUE_NAME_2 = "my.queue.2";
        private const string QUEUE_NAME_UNROUTED = "my.queue.unrouted";
        private const string EXCHANGE_FANOUT_NAME = "ex.fanout";
        private const string EXCHANGE_DIRECT_NAME = "ex.direct";

        internal static void RunExample()
        {
            // Receive messages
            using (RabbitMQ_AlternateQueue rabbitMQHelper = new RabbitMQ_AlternateQueue())
            {
                const string genericMessage = "Message with routing key";
                rabbitMQHelper.PublishMessage(EXCHANGE_DIRECT_NAME, $"{genericMessage} 'video'", "video");
                rabbitMQHelper.PublishMessage(EXCHANGE_DIRECT_NAME, $"{genericMessage} 'image'", "image");
                rabbitMQHelper.PublishMessage(EXCHANGE_DIRECT_NAME, $"{genericMessage} 'other'", "other");

                rabbitMQHelper.ActiveListeninFromQueue(QUEUE_NAME_1);
                rabbitMQHelper.ActiveListeninFromQueue(QUEUE_NAME_2);
                rabbitMQHelper.ActiveListeninFromQueue(QUEUE_NAME_UNROUTED);

                Console.WriteLine("Press any key to continue...");
                Console.ReadLine();
            }
        }

        private RabbitMQ_AlternateQueue() : base()
        {
            // Declare fanout exchange
            channel.ExchangeDeclare(
                exchange: EXCHANGE_FANOUT_NAME,
                type: "fanout",
                durable: true,
                autoDelete: false,
                arguments: null);

            // Declare direct exchange
            channel.ExchangeDeclare(
                exchange: EXCHANGE_DIRECT_NAME,
                type: "direct",
                durable: true,
                autoDelete: false,
                arguments: new Dictionary<string, object>()
                {
                    { "alternate-exchange", EXCHANGE_FANOUT_NAME }
                });

            channel.QueueDeclare(
                queue: QUEUE_NAME_1,
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: null);

            channel.QueueDeclare(
                queue: QUEUE_NAME_2,
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: null);

            channel.QueueDeclare(
                queue: QUEUE_NAME_UNROUTED,
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: null);


            // Bind queues to the exchange
            channel.QueueBind(QUEUE_NAME_1, EXCHANGE_DIRECT_NAME, "video");
            channel.QueueBind(QUEUE_NAME_2, EXCHANGE_DIRECT_NAME, "image");
            channel.QueueBind(QUEUE_NAME_UNROUTED, EXCHANGE_FANOUT_NAME, "");
        }

        public void Dispose()
        {
            DeleteExchanges(new []{ EXCHANGE_DIRECT_NAME, EXCHANGE_FANOUT_NAME });
            DeleteQueues(new[] { QUEUE_NAME_1, QUEUE_NAME_2, QUEUE_NAME_UNROUTED });
            channel.Close();
            conn.Close();
        }
    }
}
