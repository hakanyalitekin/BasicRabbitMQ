using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Text;
using System.Threading;

namespace WithoutExchange.Consumer
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

            /*Eğer Cunsumer'i ayağa kaldırdığımızda aşağıdaki kuyruk gibi bir kuyruğumuz yoksa hata alırız.
              Publisher'in aynı isimde kuyruğu oluşturduğuna eminsek yorum satırı yapabiliriz. 
              Diğer bir önemli not: Parametreleri değiştirmemeliyiz. Yani Publisher tarafında aynı isimde bir kuyruk varsa 
              ve burada da her ihtimale karşı hata alamamak adına aynı isimde kuyruk oluşturuyorsak parametreleri aynı olmalı
              aksi halde yine uygulamaız hata alır. */
            channel.QueueDeclare(queue: "logs", durable: true, exclusive: false, autoDelete: false);


            //Yukarıdaki kodlar zaten publisher kımsında yazdığımız kodlar ile birebir aynıydı.
            //Standart bağlantı-kanal-kuyruk açma kodları. Şimdi consumer ile ilgili kodlarımızı yazıyoruz.


            /*prefetchSize -> Mesaj boyutununun bizim için önemli olamadığını 0 vererek ifade ediyoruz.
              prefetchCount-> Dağıtım adetini ifade eder.
              global       -> false -> prefectCount kadar gönder örneğin 5'er5'er 
                              true  -> prefectCount'ı paylaştır. örneğin 6 mesajımız ve 2 consumer'ımız olsun 3'er 3'er ver demek
                                    yada örneğin 6 consumer varsa  1-1 ver demek */
            channel.BasicQos(prefetchSize:0, prefetchCount:1, global:false);

            //İlgili kanal üzerinden mesajlarımızı tüketecek cosumer'i oluşturuyoruz.
            var consumer = new EventingBasicConsumer(channel);

            /*Kanal üzerinden consumer'in hangi kuyruğu dinleyeceğini tanımlıyoruz.
             autoAck ->(Auto Acknowledgement) Mesajın Consumer'a ulaştıktan sonra onaylanıp onaylanmama, haliyle silinip silinmeme durumunu belirlediğimiz parametre.
             Eğer true dersek -> Mesajın doğru işlenip işlenmediğini bakılmaksızın sil demek
             Eğer false dersek -> Sen bilme ben haber vereceğim sileceğin zaman*/
            channel.BasicConsume(queue:"logs", autoAck:false, consumer:consumer);


            consumer.Received += (object sender, BasicDeliverEventArgs e) =>
            {
                var message = Encoding.UTF8.GetString(e.Body.ToArray());
                Thread.Sleep(1500);
                Console.WriteLine("Gelen Mesaj: " + message);

                /*Mesajların doğru bir şekilde işlendiğini ve kuyruktan silineceğini bildiriyoruz.
                multiple -> işlenmiş ama rabbitmq'dan silinmemiş mesajlarıda silsin istersek true demeliyiz.
                biz sadece kendi mesajımızla ilgileneceğimiz için false diyoruz.*/
                channel.BasicAck(deliveryTag:e.DeliveryTag, multiple:false);
            };

            Console.ReadKey();
        }
    }
}
