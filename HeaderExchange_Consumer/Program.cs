using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace HeaderExchange_Consumer
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

            //Yeniden declare etmek zorunda değiliz. Önce Consumer'ı ayağa kaldırırsak hata almamak için ekledik
            channel.ExchangeDeclare(exchange: "logs-header", type: ExchangeType.Headers, durable: true);


            channel.BasicQos(prefetchSize: 0, prefetchCount: 1, global: false);
            var consumer = new EventingBasicConsumer(channel);

            //Random isimli bir kuyruk tanımlıyoruz.
            var queueName = channel.QueueDeclare().QueueName;

            /* x-match zorunludur.
                    any→ değerlerden herhangi birinin eşleşmesi şartı
                    all → tüm değerlerin eşleşmesi şartı */
            Dictionary<string, object> header = new Dictionary<string, object>();
            header.Add("logLevel", "Error"); //Biz belirliyoruz
            header.Add("reportFormat", "pdf"); //Biz belirliyoruz.
            header.Add("x-match","all"); //Biz BELİRLEMİYORUZ. (any ya da all olarak karar veriyoruz.)

            //header'ımızı da ekleyip bind işlemini gerçekleştiriyoruz.
            channel.QueueBind(queue: queueName, exchange: "logs-header", routingKey: string.Empty, arguments:header);

            //Yukarıda oluşturulan queueName'i burada kullanıyoruz.
            channel.BasicConsume(queue: queueName, autoAck: false, consumer: consumer);

            Console.WriteLine("Kuyruk dinleniyor...");
            consumer.Received += (object sender, BasicDeliverEventArgs e) =>
            {
                var message = Encoding.UTF8.GetString(e.Body.ToArray());
                Thread.Sleep(1000);
                Console.WriteLine("Mail olarak gönderilen rapor : " + message);

                /*Mesajların doğru bir şekilde işlendiğini ve kuyruktan silineceğini bildiriyoruz.
                multiple -> işlenmiş ama rabbitmq'dan silinmemiş mesajlarıda silsin istersek true demeliyiz.
                biz sadece kendi mesajımızla ilgileneceğimiz için false diyoruz.*/
                channel.BasicAck(deliveryTag: e.DeliveryTag, multiple: false);
            };

            Console.ReadKey();
        }
    }
}
