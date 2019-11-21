using RabbitMQ.Client;
using RabbitMQ_ConsoleClient.Base;
using System;
using System.Text;

namespace RabbitMQ_ConsoleClient
{
    public class RabbitMQ_Fanout : RabbitMQ_Base, IDisposable
    {
        private const string QUEUE_NAME_1 = "my.queue1";
        private const string QUEUE_NAME_2 = "my.queue2";
        private const string EXCHANGE_NAME = "ex.fanout";

        public static void RunExample()
        {
            // Receive messages
            using (RabbitMQ_Fanout rabbitMQHelper = new RabbitMQ_Fanout())
            {
                rabbitMQHelper.PublishMessage(EXCHANGE_NAME, "Hi there", "");
                rabbitMQHelper.PublishMessage(EXCHANGE_NAME, "How are you?", "");

                rabbitMQHelper.ActiveListeninFromQueue(RabbitMQ_Fanout.QUEUE_NAME_1);
                rabbitMQHelper.ActiveListeninFromQueue(RabbitMQ_Fanout.QUEUE_NAME_2);
                Console.WriteLine("Press any key to continue...");
                Console.ReadLine();
            }
        }

        private RabbitMQ_Fanout(): base()
        {
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

        public void Dispose()
        {
            DeleteQueues(new[] { QUEUE_NAME_1 , QUEUE_NAME_2 });
            DeleteExchanges(new[] { EXCHANGE_NAME });
            channel.Close();
            conn.Close();
        }
    }
}
