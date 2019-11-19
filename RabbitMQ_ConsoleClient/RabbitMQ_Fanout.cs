using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Text;

namespace RabbitMQ_ConsoleClient
{
    public class RabbitMQ_Fanout : IDisposable
    {
        IConnection conn;
        IModel channel;

        private const string QUEUE_NAME_1 = "my.queue1";
        private const string QUEUE_NAME_2 = "my.queue2";
        private const string EXCHANGE_NAME = "ex.fanout";

        public static void RunExample()
        {
            // Receive messages
            using (RabbitMQ_Fanout rabbitMQHelper = new RabbitMQ_Fanout())
            {
                rabbitMQHelper.PublishMessage("Hi there");
                rabbitMQHelper.PublishMessage("How are you?");

                rabbitMQHelper.ActiveListeninFromQueue(RabbitMQ_Fanout.QUEUE_NAME_1);
                Console.WriteLine("Press any key to continue...");
                Console.ReadLine();
            }
        }

        private RabbitMQ_Fanout()
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
                type: "fanout",
                durable: true,
                autoDelete: false,
                arguments: null);

            #region Declare queues

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

            #endregion

            // Bind queues to the exchange
            channel.QueueBind(QUEUE_NAME_1, EXCHANGE_NAME, "");
            channel.QueueBind(QUEUE_NAME_2, EXCHANGE_NAME, "");
        }

        public void PublishMessage(string message)
        {
            byte[] body = Encoding.UTF8.GetBytes(message);
            channel.BasicPublish(EXCHANGE_NAME, "", null, body);
        }

        public void ActiveListeninFromQueue(string queueName)
        {
            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += Consumer_Received;

            var consumerTag = channel.BasicConsume(queueName, true, consumer);
        }

        private void Consumer_Received(object sender, BasicDeliverEventArgs e)
        {
            string message = Encoding.UTF8.GetString(e.Body);

            Console.WriteLine($"Message received: {message}");

            channel.BasicAck(e.DeliveryTag, false);
        }

        public void DeleteQueues()
        {
            channel.QueueDelete(QUEUE_NAME_1);
            channel.QueueDelete(QUEUE_NAME_2);
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
