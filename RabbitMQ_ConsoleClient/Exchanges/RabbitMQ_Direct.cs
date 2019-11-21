using RabbitMQ.Client;
using RabbitMQ_ConsoleClient.Base;
using System;
using System.Text;

namespace RabbitMQ_ConsoleClient
{
    public class RabbitMQ_Direct : RabbitMQ_Base, IDisposable
    {
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

                rabbitMQHelper.ActiveListeninFromQueue(QUEUE_NAME_INFO);
                rabbitMQHelper.ActiveListeninFromQueue(QUEUE_NAME_WARNING);
                rabbitMQHelper.ActiveListeninFromQueue(QUEUE_NAME_ERROR);
                Console.WriteLine("Press any key to continue...");
                Console.ReadLine();
            }
        }

        private RabbitMQ_Direct() : base()
        {
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

        public void Dispose()
        {
            DeleteQueues(new[] { QUEUE_NAME_INFO, QUEUE_NAME_WARNING, QUEUE_NAME_ERROR });
            DeleteExchanges(new[] { EXCHANGE_NAME });
            channel.Close();
            conn.Close();
        }
    }
}
