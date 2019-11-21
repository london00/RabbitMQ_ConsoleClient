using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Text;

namespace RabbitMQ_ConsoleClient
{
    public class RabbitMQ_Topic : IDisposable
    {
        readonly IConnection conn;
        readonly IModel channel;

        private const string QUEUE_NAME_1 = "my.queue1";
        private const string QUEUE_NAME_2 = "my.queue2";
        private const string QUEUE_NAME_3 = "my.queue3";
        private const string EXCHANGE_NAME = "ex.topic";

        public static void RunExample()
        {
            // Receive messages
            using (RabbitMQ_Topic rabbitMQHelper = new RabbitMQ_Topic())
            {
                rabbitMQHelper.PublishMessage("Hi there, how are you?", "convert.image.bpm");
                rabbitMQHelper.PublishMessage("Are you there? There is a problem!", "convert.bitmap.image");
                rabbitMQHelper.PublishMessage("The server is down!! Please come here inmediatly!", "image.bitmap.32bit");

                rabbitMQHelper.ActiveListeninFromQueue(RabbitMQ_Topic.QUEUE_NAME_1);
                rabbitMQHelper.ActiveListeninFromQueue(RabbitMQ_Topic.QUEUE_NAME_2);
                rabbitMQHelper.ActiveListeninFromQueue(RabbitMQ_Topic.QUEUE_NAME_3);
                Console.WriteLine("Press any key to continue...");
                Console.ReadLine();
            }
        }

        private RabbitMQ_Topic()
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
                type: "topic",
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

            channel.QueueDeclare(
                queue: QUEUE_NAME_3,
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: null);

            #endregion

            // Bind queues to the exchange
            channel.QueueBind(QUEUE_NAME_1, EXCHANGE_NAME, "*.image.*");
            channel.QueueBind(QUEUE_NAME_2, EXCHANGE_NAME, "#.image");
            channel.QueueBind(QUEUE_NAME_3, EXCHANGE_NAME, "image.#");
        }

        public void PublishMessage(string message, string routingKey)
        {
            byte[] body = Encoding.UTF8.GetBytes(message);
            channel.BasicPublish(EXCHANGE_NAME, routingKey, null, body);
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

            //channel.BasicAck(e.DeliveryTag, false);
        }

        public void DeleteQueues()
        {
            channel.QueueDelete(QUEUE_NAME_1);
            channel.QueueDelete(QUEUE_NAME_2);
            channel.QueueDelete(QUEUE_NAME_3);
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
