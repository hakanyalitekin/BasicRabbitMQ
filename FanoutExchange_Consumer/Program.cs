using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Text;
using System.Threading;

namespace FanoutExchange_Consumer
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


            /*RabbitMQ'nun bizim için rastgele bir isimde gecici kuyruk oluşuturmasını sağlıyoruz.
             Bu bi zorunluluk değil, bu Consumer'ı birde çok kez çalıştırıp aynı mesajları aldığını 
             göstermek için oluşturdum.*/
            #region Eğer kalıcı bir kuyruk oluşturmak isteseydik
            //var randomQueueName = "log-database-save"; //channel.QueueDeclare().QueueName;
            //channel.QueueDeclare(randomQueueName, true, false, true, null);
            #endregion
            var randomQueueName = channel.QueueDeclare().QueueName;

            //Kuyarıda tanımladığımız kuyruğu exchange'imizle eşleştiriyoruz.
            channel.QueueBind(queue: randomQueueName, exchange: "logs-fanout", routingKey: string.Empty, arguments: null);

            channel.BasicQos(prefetchSize: 0, prefetchCount: 1, global: false);
            var consumer = new EventingBasicConsumer(channel);

            //Yukarıda oluşturulan randomQueueName'i burada kullanıyoryz.
            channel.BasicConsume(queue: randomQueueName, autoAck: false, consumer: consumer);

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
