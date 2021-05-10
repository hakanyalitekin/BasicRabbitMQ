using RabbitMQ.Client;
using System;
using System.Linq;
using System.Text;

namespace WithoutExchange.Publisher
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

            //Yukarda oluşturduğumuza bağlantı üzerinden bir kanal oluşturuyoruz.
            IModel channel = connection.CreateModel();

            /* Oluşturduğumuz kanal üzerinden bir kuyruk yaratıyoruz. QueueDeclare'nin parametreleri sırasıyla;
             * queue -> Kuyruğumuzun adı
             * durable(dayanıklı) -> Eğer true yaparsak kuyruklarımız fiziksel, false yaparsak hafızada saklanır.
             * exclusive(özel) ->
                True sadece bu kanal, false farklı kanallar üzerinden de erişim demek.
                Kuyruğumuza Consumer uygulaması üzerinden, yani farklı bir kanal üzerinden erişeceğimiz için false yapmalıyız.
             *autoDelete -> Kuyrukta yer alan veri consumer'a ulaştığında silinmesi belirtilir. */
            channel.QueueDeclare(queue: "logs", durable: true, exclusive: false, autoDelete: false);

            Enumerable.Range(1, 50).ToList().ForEach(x =>
            {

                //Mesajımızı oluşuturup, byte array'e çeviriyoruz.
                string logMessage = $"LogID -> {x}";
                var logMessageBody = Encoding.UTF8.GetBytes(logMessage);

                /*Mesajlarımızı doğrudan kuyruğa ekleyeceğimiz için yani bir exchange routingi
                 kullanmadığımız için string.Empty ile boş gönderiyoruz. Daha sonra kuyruğumuzun adını yazıyoruz.
                 propertyleriyle ilgilenmeceğimiz için null deyip ardından mesajımızı parametre olarak ekliyoruz.*/
                channel.BasicPublish(string.Empty, "logs", null, logMessageBody);

                Console.WriteLine($"Log kuyruğa gönderilmiştir: {logMessage}");
            });

            Console.ReadKey();
        }
    }
}
