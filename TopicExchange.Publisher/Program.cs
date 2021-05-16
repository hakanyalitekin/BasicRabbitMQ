using RabbitMQ.Client;
using System;
using System.Linq;
using System.Text;

namespace TopicExchange.Publisher
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

            //Exchange adımızı ve tipimizi aşağıdaki gibi düzenliyoruz.
            channel.ExchangeDeclare(exchange: "logs-topic", type: ExchangeType.Topic, durable: true);

            Enumerable.Range(1, 20).ToList().ForEach(x =>
            {
                /*Topic senaryo gereği 3 çeşit rastgele tiplerten ve mesajlardan oluşan toplam 20 adet mesaj üretiyoruz.*/
                Random rnm = new Random();
                LogTypes log1 = (LogTypes)rnm.Next(1, 4);
                LogTypes log2 = (LogTypes)rnm.Next(1, 4);
                LogTypes log3 = (LogTypes)rnm.Next(1, 4);

                //Mesajımızı ve RouteKey'imizi Topic senaryosuna göre güncelliyoruz.
                var routeKey = $"{log1}.{log2}.{log3}";
                string logMessage = $"Log Types -> {log1}-{log2}-{log3}";

                //Mesajımızı oluşuturup, byte array'e çeviriyoruz.
                var logMessageBody = Encoding.UTF8.GetBytes(logMessage);

                /*Route Key'i logs-topic olarak güncelliyoruz. */
                channel.BasicPublish("logs-topic", routeKey, null, logMessageBody);

                Console.WriteLine($"Log kuyruğa gönderilmiştir: {logMessage}");
            });

            Console.ReadKey();
        }
    }
    //Senaryo gereği oluşturduğumuz enum
    enum LogTypes { Error = 1, Warning = 2, Info = 3 }
}
