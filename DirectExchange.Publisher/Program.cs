using RabbitMQ.Client;
using System;
using System.Linq;
using System.Text;

namespace DirectExchange.Publisher
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
            channel.ExchangeDeclare(exchange: "logs-direct", type: ExchangeType.Direct, durable: true);

            /*Bir önceki Fanout örneğinde kuyruk deklare etme işini Consumer tarafına bırakmıştık.
             Burada ise Publisher tarafında oluşturup, yine bind etme işlemini da burada gerçekleştirdik.
             En aşağıda -LogTypes- adında bir enum tanımladık ve içerisene 3 tip hata ekledik. 
             Bu 3 hata arasında dönüp 3 adet kuyruk oluşturup bind işlemini gerçekleştiriyoruz.
            */
            Enum.GetNames(typeof(LogTypes)).ToList().ForEach(x =>
            {
                var routeKey = $"route-{x}";
                var queueName = $"direct-queue-{x}";

                //Kuruğumuzu oluşturuyoruz.
                channel.QueueDeclare(queue: queueName, durable: true, exclusive: false, autoDelete: false);

                //Oluştuduğumuz kuruk ile exchange'imizi routeKey tanımlaması yaparak bind ediyoruz.
                channel.QueueBind(queue: queueName, exchange: "logs-direct", routeKey, null);
            });

            Enumerable.Range(1, 20).ToList().ForEach(x =>
            {
                /*Senaryo gereği rastgele tiplerten ve mesajlardan oluşan toplam 20 adet mesaj üretiyoruz.*/
                LogTypes logType = (LogTypes)new Random().Next(1, 4);
                string logMessage = $"Log Type -> {logType}";
                var routeKey = $"route-{logType}";

                //Mesajımızı oluşuturup, byte array'e çeviriyoruz.
                var logMessageBody = Encoding.UTF8.GetBytes(logMessage);

                /*İlk iki örneğimizde RoutingKey'i boş geçiyorduk artık
                 yukarıda oluşan routeKey'i kullanıyoruz*/
                channel.BasicPublish("logs-direct", routeKey, null, logMessageBody);

                Console.WriteLine($"Log kuyruğa gönderilmiştir: {logMessage}");
            });

            Console.ReadKey();
        }
    }
    //Senaryo gereği oluşturduğumuz enum
    enum LogTypes { Error = 1, Warning = 2, Info = 3 }
}
