using Confluent.Kafka;
using KafkaNet;
using KafkaNet.Model;
using KafkaNet.Protocol;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace Consumer
{
    class Program
    {
        static void Main(string[] args)
        {
            #region KafkaNet
            //string topic = "IDGTestTopic";
            //Uri uri = new Uri("http://localhost:9092");
            //var options = new KafkaOptions(uri);
            //var broker = new BrokerRouter(options);
            //OffsetPosition[] position = new OffsetPosition[] { };
            //var cliente = new KafkaNet.Consumer(new ConsumerOptions(topic, broker));

            //List<ObjetoTeste> list = new List<ObjetoTeste>();
            //foreach(var message in cliente.Consume())
            //{
            //    ObjetoTeste obj = new ObjetoTeste();
            //    Console.WriteLine(Encoding.UTF8.GetString(message.Value));
            //    //var commit = new OffsetCommitRequest
            //    //{
            //    //    OffsetCommits = new List<OffsetCommit>
            //    //    {
            //    //        new OffsetCommit
            //    //        {
            //    //            PartitionId = message.Meta.PartitionId,
            //    //            Topic = topic,
            //    //            Offset = message.Meta.Offset,
            //    //            Metadata = message.Meta.ToString()
            //    //        }
            //    //    }
            //    //};
            //    //var r = broker.SelectBrokerRoute(topic, message.Meta.PartitionId);
            //    //r.Connection.SendAsync(commit);
            //    obj = JsonConvert.DeserializeObject<ObjetoTeste>(Encoding.UTF8.GetString(message.Value));
            //    list.Add(obj);
            //}

            //Console.WriteLine(list.Count.ToString());
            //Console.ReadLine();
            #endregion

            #region Confluent
            var config = new ConsumerConfig
            {
                BootstrapServers = "localhost:9092",
                GroupId = "testee",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            const int commitPeriod = 5;

            using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
            {
                consumer.Subscribe("testeConfluent");
                CancellationTokenSource cts = new CancellationTokenSource();
                Console.CancelKeyPress += (_, e) => {
                    e.Cancel = true; // prevent the process from terminating.
                    cts.Cancel();
                };

                while (true)
                {
                    var consumeResult = consumer.Consume(cts.Token);
                    if (consumeResult.IsPartitionEOF)
                    {
                        Console.WriteLine($"Reached end of topic {consumeResult.Topic}, partition {consumeResult.Partition}, offset {consumeResult.Offset}");
                        continue;
                    }
                    Console.WriteLine($"Received message at {consumeResult.TopicPartitionOffset}: {consumeResult.Value}");

                    if (consumeResult.Offset % commitPeriod == 0)
                    {
                        // The Commit method sends a "commit offsets" request to the Kafka
                        // cluster and synchronously waits for the response. This is very
                        // slow compared to the rate at which the consumer is capable of
                        // consuming messages. A high performance application will typically
                        // commit offsets relatively infrequently and be designed handle
                        // duplicate messages in the event of failure.
                        try
                        {
                            consumer.Commit(consumeResult);
                        }
                        catch (KafkaException e)
                        {
                            Console.WriteLine($"Commit error: {e.Error.Reason}");
                        }
                    }
                }
            }
            #endregion

        }
    }

    class ObjetoTeste
    {

        public int Codigo { get; set; }
        public string Descricao { get; set; }
    }
}
