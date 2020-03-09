using Confluent.Kafka;
using KafkaNet;
using KafkaNet.Model;
using KafkaNet.Protocol;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Net;
using System.Text;

namespace Producer
{
    class Program
    {
        static void Main(string[] args)
        {
            #region KafkaNet
            //string payLoad = "Welcome to Kafka";
            //ObjetoTeste obj = new ObjetoTeste();
            //obj.Codigo = 1;
            //obj.Descricao = "Teste";
            //string topic = "IDGTestTopic";
            //Message msg = new Message(Serialize(obj));
            //Uri uri = new Uri("http://localhost:9093");
            //var options = new KafkaOptions(uri);
            //var router = new BrokerRouter(options);
            //var client = new KafkaNet.Producer(router);
            //client.SendMessageAsync(topic, new List<Message> { msg }).Wait();
            //Console.ReadLine();
            #endregion

            #region Confluent
            var config = new ProducerConfig { BootstrapServers = "localhost:9092" };
            
            using (var producer = new ProducerBuilder<Null, string>(config).Build())
            {
                var t = producer.ProduceAsync("testeConfluent", new Message<Null, string> { Value = "teste2"});
                t.ContinueWith(task =>
                {
                    if (task.IsFaulted)
                    {
                        throw new Exception("Errrrrrrrrrrrro");
                    }
                    else
                    {
                        Console.WriteLine($"Wrote to offset: {task.Result.Offset}");
                        Console.ReadLine();
                    }
                });
            }
            Console.ReadLine();

            #endregion
        }

    private static string Serialize(ObjetoTeste obj)
    {
        string vs = null;
        vs = JsonConvert.SerializeObject(obj);
        return vs;
    }
}

class ObjetoTeste
{

    public int Codigo { get; set; }
    public string Descricao { get; set; }
}
}
