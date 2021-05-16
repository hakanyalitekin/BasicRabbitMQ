using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Text;
using System.Threading;

namespace TopicExchange.Consumer
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

            channel.BasicQos(prefetchSize: 0, prefetchCount: 1, global: false);
            var consumer = new EventingBasicConsumer(channel);

            Console.WriteLine("Route Key'in ortasında Error olan kuyruklar dinleniyor.");
            //Random isimli bir kuyruk tanımlıyoruz.
            var queueName = channel.QueueDeclare().QueueName;

            //Başı ve sonu önemli değil ortasında error yazanlar gelsin istiyoruz.
            var routeKey = "*.Error.*";
            //var routeKey = "Info.*.Error"; ==> Başı Info sonu Error olanlar.
            //var routeKey = "#.Info"; ==> Başın ne olduğu önemli değil sonu Info olanlar.

            //Bind işlemini gerçekleştiriyoruz.
            channel.QueueBind(queue: queueName, exchange: "logs-topic", routingKey: routeKey);

            //Yukarıda oluşturulan queueName'i burada kullanıyoruz.
            channel.BasicConsume(queue: queueName, autoAck: false, consumer: consumer);

            consumer.Received += (object sender, BasicDeliverEventArgs e) =>
            {
                var message = Encoding.UTF8.GetString(e.Body.ToArray());
                Thread.Sleep(1000);
                Console.WriteLine("Gelen Mesaj: " + message);

                /*Mesajların doğru bir şekilde işlendiğini ve kuyruktan silineceğini bildiriyoruz.
                multiple -> işlenmiş ama rabbitmq'dan silinmemiş mesajlarıda silsin istersek true demeliyiz.
                biz sadece kendi mesajımızla ilgileneceğimiz için false diyoruz.*/
                channel.BasicAck(deliveryTag: e.DeliveryTag, multiple: false);
            };

            Console.ReadKey();
        }
    }
}
