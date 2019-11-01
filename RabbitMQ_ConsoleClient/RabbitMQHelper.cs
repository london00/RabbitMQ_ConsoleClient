using RabbitMQ.Client;
using System;
using System.Text;

namespace RabbitMQ_ConsoleClient
{
    public class RabbitMQHelper: IDisposable
    {
        private const string QUEUE_NAME_1 = "my.queue1";
        private const string QUEUE_NAME_2 = "my.queue2";
        private const string EXCHANGE_NAME = "ex.fanout";

        IConnection conn;
        IModel channel;

        public RabbitMQHelper()
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

        public void DeleteQueues() {
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
