using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Text;

namespace RabbitMQ_ConsoleClient
{
    public class RabbitMQ_Direct : IDisposable
    {
        IConnection conn;
        IModel channel;

        private const string QUEUE_NAME_INFO = "my.queue.info";
        private const string QUEUE_NAME_WARNING = "my.queue.warning";
        private const string QUEUE_NAME_ERROR = "my.queue.direct";
        private const string EXCHANGE_NAME = "ex.direct";

        internal static void RunExample()
        {
            // Receive messages
            using (RabbitMQ_Direct rabbitMQHelper = new RabbitMQ_Direct())
            {
                rabbitMQHelper.PublishMessage("Hi there, how are you?", "info");
                rabbitMQHelper.PublishMessage("Are you there? There is a problem!", "warning");
                rabbitMQHelper.PublishMessage("The server is down!! Please come here inmediatly!", "error");

                rabbitMQHelper.ActiveListeninFromQueue();
                Console.WriteLine("Press any key to continue...");
                Console.ReadLine();
            }
        }
        
        private RabbitMQ_Direct()
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

            // Declare exchange
            channel.ExchangeDeclare(
                exchange: EXCHANGE_NAME,
                type: "direct",
                durable: true,
                autoDelete: false,
                arguments: null);

            channel.QueueDeclare(
                queue: QUEUE_NAME_INFO,
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: null);

            channel.QueueDeclare(
                queue: QUEUE_NAME_WARNING,
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: null);

            channel.QueueDeclare(
                queue: QUEUE_NAME_ERROR,
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: null);


            // Bind queues to the exchange
            channel.QueueBind(QUEUE_NAME_INFO, EXCHANGE_NAME, "info");
            channel.QueueBind(QUEUE_NAME_WARNING, EXCHANGE_NAME, "warning");
            channel.QueueBind(QUEUE_NAME_ERROR, EXCHANGE_NAME, "error");
        }

        public void PublishMessage(string message, string routingKey)
        {
            byte[] body = Encoding.UTF8.GetBytes(message);
            channel.BasicPublish(EXCHANGE_NAME, routingKey, null, body);
        }

        public void ActiveListeninFromQueue()
        {
            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += Consumer_Received;

            var consumerTag1 = channel.BasicConsume(QUEUE_NAME_INFO, true, consumer);
            var consumerTag2 = channel.BasicConsume(QUEUE_NAME_WARNING, true, consumer);
            var consumerTag3 = channel.BasicConsume(QUEUE_NAME_ERROR, true, consumer);
        }

        private void Consumer_Received(object sender, BasicDeliverEventArgs e)
        {
            string message = Encoding.UTF8.GetString(e.Body);

            Console.WriteLine($"Message received: [Exchange: {e.Exchange}] [RoutingKey: {e.RoutingKey}] ---> {message}");

            //channel.BasicAck(e.DeliveryTag, false);
        }

        public void DeleteQueues()
        {
            channel.QueueDelete(QUEUE_NAME_INFO);
            channel.QueueDelete(QUEUE_NAME_WARNING);
            channel.QueueDelete(QUEUE_NAME_ERROR);
        }

        public void DeleteExchanges()
        {
            channel.ExchangeDelete(EXCHANGE_NAME);
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
