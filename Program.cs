using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RestApiStudy.Models;
using RestApiStudy.Repositories;
using System;
using System.Text;

namespace RabbitMQConsumer
{
    class Program
    {
        static void Main(string[] args)
        {
            ConnectionFactory factory = new ConnectionFactory();
            factory.Uri = new System.Uri("amqp://guest:guest@localhost:5672");

            IConnection conn = factory.CreateConnection();
            IModel channel = conn.CreateModel();

            channel.QueueDeclare("queue",
                durable: true,
                autoDelete: false,
                arguments: null,
                exclusive: false
                );

            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += (sender, e) =>
            {
                var body = e.Body.ToArray();
                var personData = Encoding.UTF8.GetString(body);
                var _person = JsonConvert.DeserializeObject<Person>(personData);

                IPersonDatabaseSettings pds = new PersonDatabaseSettings
                {
                    ConnectionString = "mongodb://localhost:27017",
                    DatabaseName = "UserDb",
                    CollectionName = "Person"
                };
                try
                {
                    PersonRepository repo = new PersonRepository(pds);
                    var isAdded = repo.AddPerson(_person).Result;
                    Console.WriteLine(isAdded);
                }
                catch (System.Exception ex)
                {

                    Console.WriteLine(ex.Message);
                }

            };
            channel.BasicConsume("queue", true, consumer);
            Console.ReadLine();
            channel.Close();
            conn.Close();
        }
    }
}
