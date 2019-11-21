using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Text;

namespace RabbitMQ_ConsoleClient.Alternate
{
    public class RabbitMQ_AlternateQueue : IDisposable
    {
        private readonly IConnection conn;
        private readonly IModel channel;
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
                rabbitMQHelper.PublishMessage($"{genericMessage} 'video'", "video");
                rabbitMQHelper.PublishMessage($"{genericMessage} 'image'", "image");
                rabbitMQHelper.PublishMessage($"{genericMessage} 'other'", "other");

                rabbitMQHelper.ActiveListeninFromQueue();
                Console.WriteLine("Press any key to continue...");
                Console.ReadLine();
            }
        }

        private RabbitMQ_AlternateQueue()
        {
            var factory = new ConnectionFactory
            {
                HostName = "localhost",
                VirtualHost = "/",
                Port = 5672,
                UserName = "guest",
                Password = "guest"
            };

            conn = factory.CreateConnection();
            channel = conn.CreateModel();

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

        public void PublishMessage(string message, string routingKey)
        {
            byte[] body = Encoding.UTF8.GetBytes(message);
            channel.BasicPublish(EXCHANGE_DIRECT_NAME, routingKey, null, body);
        }

        public void ActiveListeninFromQueue()
        {
            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += Consumer_Received;

            var consumerTag1 = channel.BasicConsume(QUEUE_NAME_1, true, consumer);
            var consumerTag2 = channel.BasicConsume(QUEUE_NAME_2, true, consumer);
            var consumerTag3 = channel.BasicConsume(QUEUE_NAME_UNROUTED, true, consumer);
        }

        private void Consumer_Received(object sender, BasicDeliverEventArgs e)
        {
            string message = Encoding.UTF8.GetString(e.Body);

            Console.WriteLine($"Message received: [Exchange: {e.Exchange}] [RoutingKey: {e.RoutingKey}] ---> {message}");

            //channel.BasicAck(e.DeliveryTag, false);
        }

        public void DeleteQueues()
        {
            channel.QueueDelete(QUEUE_NAME_1);
            channel.QueueDelete(QUEUE_NAME_UNROUTED);
        }

        public void DeleteExchanges()
        {
            channel.ExchangeDelete(EXCHANGE_DIRECT_NAME);
        }

        public void Dispose()
        {
            DeleteQueues();
            DeleteExchanges();
            channel.Close();
            conn.Close();
        }
    }
}
