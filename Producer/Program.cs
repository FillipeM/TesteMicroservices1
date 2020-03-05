using KafkaNet;
using KafkaNet.Model;
using KafkaNet.Protocol;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;

namespace Producer
{
    class Program
    {
        static void Main(string[] args)
        {
            string payLoad = "Welcome to Kafka";
            ObjetoTeste obj = new ObjetoTeste();
            obj.Codigo = 1;
            obj.Descricao = "Teste";
            string topic = "IDGTestTopic";
            Message msg = new Message(Serialize(obj));
            Uri uri = new Uri("http://localhost:9093");
            var options = new KafkaOptions(uri);
            var router = new BrokerRouter(options);
            var client = new KafkaNet.Producer(router);
            client.SendMessageAsync(topic, new List<Message> { msg }).Wait();
            Console.ReadLine();
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
