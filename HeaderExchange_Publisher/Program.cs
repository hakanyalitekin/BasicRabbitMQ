using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace HeaderExchange_Publisher
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

            //Exchange adımızı ve tipimizi aşağıdaki gibi düzenliyoruz.
            channel.ExchangeDeclare(exchange: "logs-header", type: ExchangeType.Headers, durable: true);

            //Mesajımızı iletirken header de göndermek üzere bir key-value obje oluşturalım.
            Dictionary<string, object> header = new Dictionary<string, object>();
            header.Add("logLevel", "Error");
            header.Add("reportFormat", "pdf");

            //Oluşturulan header'ı, aşağıdaki gibi Headers'a eşitleyelim
            var properties = channel.CreateBasicProperties();
            properties.Headers = header;

            //Örnek bir rapor içeriği olduğunu varsayalım.
            var message = Encoding.UTF8.GetBytes("----Kritik_Hata_Raporu.pdf----");

            /*Exchange tipimizi belirtiyoruz, route key kullanamdığımız için boş geçiyoruz.
             mesajımızın header'ını doldurduğumuz için yukarıda tanımladığımız propertiyi gönderiyoruz.
            Son olarakta mesajımızı ekliyoruz. */
            channel.BasicPublish(exchange:"logs-header",routingKey:string.Empty, basicProperties:properties, body : message);

            Console.WriteLine("Kritik_Hata_Raporu.pdf RabbitMQ'ya gönderilmiştir.");
            Console.ReadKey();
        }
    }
}
