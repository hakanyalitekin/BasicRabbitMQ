using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Text;
using System.Threading;

namespace DirectExchange.Consumer
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

            //Kullanıcın dinlemek istediği tipi belirlemesini sağlıyoruz.
            Console.WriteLine("Dinlemek istediğiniz log tipini(Error,Warning,Info) belirtiniz:");
            var selectedType = Console.ReadLine();
            var queueName = $"direct-queue-{selectedType}";

            //Yukarıda oluşturulan queueName'i burada kullanıyoryz.
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
