using RabbitMQ.Client;
using System;
using System.Linq;
using System.Text;

namespace FanoutExchange_Publisher
{
    class Program
    {
        static void Main(string[] args)
        {
            //Öncelikle RabbitMQ'ya bağlanıyoruz.
            var factory = new ConnectionFactory();
            factory.Uri = new Uri("amqp://guest:guest@localhost:5672");

            //Bağlantı açıyoruz
            using var connection = factory.CreateConnection();

            IModel channel = connection.CreateModel();

            //channel.QueueDeclare(queue: "logs", durable: true, exclusive: false, autoDelete: false);

            channel.ExchangeDeclare(exchange:"logs-fanout", type:ExchangeType.Fanout, durable:true);

            Enumerable.Range(1, 20).ToList().ForEach(x =>
            {
                //Mesajımızı oluşuturup, byte array'e çeviriyoruz.
                string logMessage = $"LogID -> {x}";
                var logMessageBody = Encoding.UTF8.GetBytes(logMessage);

                /* Fanout tipli exchange'lerimiz mesajlarımız herhangi bir routing key'imiz  
                 olmadığı için ikinci parametremizi boş bırakıyoruz (string.Empty).Bir önceki 
                 örneğimizde ilk parametre boş ikinci parametre yani queue logs'tu */
                channel.BasicPublish("logs-fanout", string.Empty, null, logMessageBody);

                Console.WriteLine($"Log kuyruğa gönderilmiştir: {logMessage}");
            });

            Console.ReadKey();
        }
    }
}
