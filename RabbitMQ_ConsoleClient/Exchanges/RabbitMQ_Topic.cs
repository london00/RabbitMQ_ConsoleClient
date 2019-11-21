using RabbitMQ.Client;
using RabbitMQ_ConsoleClient.Base;
using System;

namespace RabbitMQ_ConsoleClient
{
    public class RabbitMQ_Topic : RabbitMQ_Base, IDisposable
    {
        private const string QUEUE_NAME_1 = "my.queue1";
        private const string QUEUE_NAME_2 = "my.queue2";
        private const string QUEUE_NAME_3 = "my.queue3";
        private const string EXCHANGE_NAME = "ex.topic";

        public static void RunExample()
        {
            // Receive messages
            using (RabbitMQ_Topic rabbitMQHelper = new RabbitMQ_Topic())
            {
                rabbitMQHelper.PublishMessage(EXCHANGE_NAME, "Hi there, how are you?", "convert.image.bpm");
                rabbitMQHelper.PublishMessage(EXCHANGE_NAME, "Are you there? There is a problem!", "convert.bitmap.image");
                rabbitMQHelper.PublishMessage(EXCHANGE_NAME, "The server is down!! Please come here inmediatly!", "image.bitmap.32bit");

                rabbitMQHelper.ActiveListeninFromQueue(RabbitMQ_Topic.QUEUE_NAME_1);
                rabbitMQHelper.ActiveListeninFromQueue(RabbitMQ_Topic.QUEUE_NAME_2);
                rabbitMQHelper.ActiveListeninFromQueue(RabbitMQ_Topic.QUEUE_NAME_3);
                Console.WriteLine("Press any key to continue...");
                Console.ReadLine();
            }
        }

        private RabbitMQ_Topic(): base()
        {
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

        public void Dispose()
        {
            DeleteQueues(new[] { QUEUE_NAME_1, QUEUE_NAME_2, QUEUE_NAME_3 });
            DeleteExchanges(new[] { EXCHANGE_NAME });
            channel.Close();
            conn.Close();
        }
    }
}
