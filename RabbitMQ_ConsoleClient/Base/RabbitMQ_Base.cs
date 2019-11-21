using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Text;

namespace RabbitMQ_ConsoleClient.Base
{
    public abstract class RabbitMQ_Base
    {
        protected readonly IConnection conn;
        protected readonly IModel channel;

        protected RabbitMQ_Base()
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
        }

        protected void PublishMessage(string exchangeName, string message, string routingKey)
        {
            byte[] body = Encoding.UTF8.GetBytes(message);
            channel.BasicPublish(exchangeName, routingKey, null, body);
        }

        protected void Consumer_Received(object sender, BasicDeliverEventArgs e)
        {
            string message = Encoding.UTF8.GetString(e.Body);

            Console.WriteLine($"Message received: [Exchange: {e.Exchange}] [RoutingKey: {e.RoutingKey}] ---> {message}");

            //channel.BasicAck(e.DeliveryTag, false);
        }

        protected void DeleteQueues(string[] queues)
        {
            foreach (var queue in queues)
            {
                channel.QueueDelete(queue);
            }
        }

        protected void DeleteExchanges(string[] exchanges)
        {
            foreach (var exchange in exchanges)
            {
                channel.ExchangeDelete(exchange);
            }
        }

        protected void ActiveListeninFromQueue(string queueName)
        {
            Console.WriteLine($"Queue name [{queueName}]");
            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += Consumer_Received;

            var consumerTag = channel.BasicConsume(queueName, true, consumer);
        }
    }
}
