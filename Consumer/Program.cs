using KafkaNet;
using KafkaNet.Model;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;

namespace Consumer
{
    class Program
    {
        static void Main(string[] args)
        {
            string topic = "IDGTestTopic";
            Uri uri = new Uri("http://localhost:9093");
            var options = new KafkaOptions(uri);
            var broker = new BrokerRouter(options);
            var cliente = new KafkaNet.Consumer(new ConsumerOptions(topic, broker));

            List<ObjetoTeste> list = new List<ObjetoTeste>();

            foreach(var message in cliente.Consume())
            {
                ObjetoTeste obj = new ObjetoTeste();
                Console.WriteLine(Encoding.UTF8.GetString(message.Value));
                obj = JsonConvert.DeserializeObject<ObjetoTeste>(Encoding.UTF8.GetString(message.Value));
                list.Add(obj);
            }

            Console.WriteLine(list.Count.ToString());
            Console.ReadLine();
        }
    }

    class ObjetoTeste
    {

        public int Codigo { get; set; }
        public string Descricao { get; set; }
    }
}
