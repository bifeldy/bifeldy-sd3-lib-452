/**
 * 
 * Author       :: Basilius Bias Astho Christyono
 * Phone        :: (+62) 889 236 6466
 * 
 * Department   :: IT SD 03
 * Mail         :: bias@indomaret.co.id
 * 
 * Catatan      :: Kafka Pub-Sub
 *              :: Harap Didaftarkan Ke DI Container
 * 
 */

using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka;
using Confluent.Kafka.Admin;

using bifeldy_sd3_lib_452.Handlers;
using bifeldy_sd3_lib_452.Models;

namespace bifeldy_sd3_lib_452.Utilities {

    public interface IKafka {
        Task CreateTopicIfNotExist(string hostPort, string topicName, short replication = -1, int partition = -1);
        Task<List<KafkaDeliveryResult<string, string>>> ProduceSingleMultipleMessages(string hostPort, string topicName, List<KafkaMessage<string, dynamic>> data, short replication = -1, int partition = -1);
        Task<List<KafkaMessage<string, T>>> ConsumeSingleMultipleMessages<T>(string hostPort, string groupId, string topicName, short replication = -1, int partition = -1, long offset = -1, ulong nMessagesBlock = 1);
        void CreateKafkaProducerListener(string hostPort, string topicName, short replication = -1, int partition = -1, bool suffixKodeDc = false, CancellationToken stoppingToken = default, string pubSubName = null);
        void DisposeAndRemoveKafkaProducerListener(string hostPort, string topicName, bool suffixKodeDc = false, string pubSubName = null);
        void CreateKafkaConsumerListener<T>(string hostPort, string topicName, string groupId, short replication = -1, int partition = -1, bool suffixKodeDc = false, CancellationToken stoppingToken = default, Action<KafkaMessage<string, T>> execLambda = null, string pubSubName = null);
        Dictionary<string, T> MessageToDictionary<T>(object obj);
    }

    public sealed class CKafka : IKafka {

        private readonly ILogger _logger;
        private readonly IConverter _converter;
        private readonly IPubSub _pubSub;
        private readonly IDbHandler _db;

        TimeSpan timeout = TimeSpan.FromSeconds(10);

        readonly IDictionary<string, dynamic> keyValuePairs = new ExpandoObject();

        public CKafka(ILogger logger, IConverter converter, IPubSub pubSub, IDbHandler db) {
            this._logger = logger;
            this._converter = converter;
            this._pubSub = pubSub;
            this._db = db;
        }

        public async Task CreateTopicIfNotExist(string hostPort, string topicName, short replication = -1, int partition = -1) {
            try {
                var adminConfig = new AdminClientConfig {
                    BootstrapServers = hostPort
                };
                using (IAdminClient adminClient = new AdminClientBuilder(adminConfig).Build()) {
                    Metadata metadata = adminClient.GetMetadata(this.timeout);
                    List<TopicMetadata> topicsMetadata = metadata.Topics;
                    bool isExist = metadata.Topics.Select(a => a.Topic).Contains(topicName);
                    if (!isExist) {
                        await adminClient.CreateTopicsAsync(new List<TopicSpecification> {
                            new TopicSpecification { Name = topicName, ReplicationFactor = replication, NumPartitions = partition }
                        });
                    }
                }
            }
            catch (Exception ex) {
                this._logger.WriteError(ex);
            }
        }

        private Headers CreateHeaderFromDictionary(IDictionary<string, string> dict) {
            var hdr = new Headers();
            foreach (KeyValuePair<string, string> kvp in dict) {
                if (kvp.Value != null) {
                    hdr.Add(kvp.Key, Encoding.UTF8.GetBytes(kvp.Value));
                }
            }

            return hdr;
        }

        private IDictionary<string, string> CreateDictionaryFromHeaders(Headers headers) {
            return headers.Select(h => {
                string key = h.Key;
                string value = Encoding.UTF8.GetString(h.GetValueBytes());
                return new { key, value };
            }).ToDictionary(h => h.key, h => h.value);
        }

        private ProducerConfig GenerateKafkaProducerConfig(string hostPort) {
            return new ProducerConfig {
                BootstrapServers = hostPort
            };
        }

        private IProducer<T1, T2> GenerateProducerBuilder<T1, T2>(ProducerConfig config) {
            return new ProducerBuilder<T1, T2>(config).Build();
        }

        private IProducer<T1, T2> CreateKafkaProducerInstance<T1, T2>(string hostPort) {
            return this.GenerateProducerBuilder<T1, T2>(this.GenerateKafkaProducerConfig(hostPort));
        }

        public async Task<List<KafkaDeliveryResult<string, string>>> ProduceSingleMultipleMessages(string hostPort, string topicName, List<KafkaMessage<string, dynamic>> data, short replication = -1, int partition = -1) {
            await this.CreateTopicIfNotExist(hostPort, topicName, replication, partition);
            using (IProducer<string, string> producer = this.CreateKafkaProducerInstance<string, string>(hostPort)) {
                var results = new List<KafkaDeliveryResult<string, string>>();
                foreach(KafkaMessage<string, dynamic> d in data) {
                    Headers x = this.CreateHeaderFromDictionary(d.Headers);
                    var msg = new Message<string, string> {
                        Headers = x,
                        Key = d.Key,
                        Timestamp = d.Timestamp,
                        Value = typeof(string) == d.Value.GetType() ? d.Value : this._converter.ObjectToJson(d.Value)
                    };
                    DeliveryResult<string, string> result = await producer.ProduceAsync(topicName, msg);
                    this._logger.WriteInfo($"{this.GetType().Name}Produce{result.Status}", $"{msg.Key} :: {msg.Value}");
                    results.Add(new KafkaDeliveryResult<string, string> {
                        Headers = this.CreateDictionaryFromHeaders(result.Headers),
                        Key = result.Key,
                        Message = new KafkaMessage<string, string> {
                            Headers = this.CreateDictionaryFromHeaders(result.Message.Headers),
                            Key = result.Message.Key,
                            Timestamp = result.Message.Timestamp,
                            Value = result.Message.Value
                        },
                        Offset = result.Offset,
                        Partition = result.Partition,
                        Status = result.Status,
                        Timestamp = result.Timestamp,
                        Topic = result.Topic,
                        TopicPartition = result.TopicPartition,
                        TopicPartitionOffset = result.TopicPartitionOffset
                    });
                }

                return results;
            }
        }

        private ConsumerConfig GenerateKafkaConsumerConfig(string hostPort, string groupId, AutoOffsetReset autoOffsetReset) {
            return new ConsumerConfig {
                BootstrapServers = hostPort,
                GroupId = groupId,
                AutoOffsetReset = autoOffsetReset,
                EnableAutoCommit = false
            };
        }

        private IConsumer<T1, T2> GenerateConsumerBuilder<T1, T2>(ConsumerConfig config) {
            return new ConsumerBuilder<T1, T2>(config).Build();
        }

        private IConsumer<T1, T2> CreateKafkaConsumerInstance<T1, T2>(string hostPort, string groupId) {
            return this.GenerateConsumerBuilder<T1, T2>(this.GenerateKafkaConsumerConfig(hostPort, groupId, AutoOffsetReset.Earliest));
        }

        private TopicPartition CreateKafkaConsumerTopicPartition(string topicName, int partition) {
            return new TopicPartition(topicName, Math.Max(Partition.Any, partition));
        }

        private TopicPartitionOffset CreateKafkaConsumerTopicPartitionOffset(TopicPartition topicPartition, long offset) {
            return new TopicPartitionOffset(topicPartition, new Offset(offset));
        }

        public async Task<List<KafkaMessage<string, T>>> ConsumeSingleMultipleMessages<T>(string hostPort, string groupId, string topicName, short replication = -1, int partition = -1, long offset = -1, ulong nMessagesBlock = 1) {
            await this.CreateTopicIfNotExist(hostPort, topicName, replication, partition);
            using (IConsumer<string, string> consumer = this.CreateKafkaConsumerInstance<string, string>(hostPort, groupId)) {
                TopicPartition topicPartition = this.CreateKafkaConsumerTopicPartition(topicName, partition);
                if (offset < 0) {
                    WatermarkOffsets watermarkOffsets = consumer.QueryWatermarkOffsets(topicPartition, this.timeout);
                    offset = watermarkOffsets.High.Value - 1;
                }

                TopicPartitionOffset topicPartitionOffset = this.CreateKafkaConsumerTopicPartitionOffset(topicPartition, offset);
                consumer.Assign(topicPartitionOffset);
                var results = new List<KafkaMessage<string, T>>();
                for (ulong i = 0; i < nMessagesBlock; i++) {
                    ConsumeResult<string, string> result = consumer.Consume(this.timeout);
                    this._logger.WriteInfo($"{this.GetType().Name}Consume", $"{result.Message.Key} :: {result.Message.Value}");
                    var message = new KafkaMessage<string, T> {
                        Headers = this.MessageToDictionary<string>(result.Message.Headers),
                        Key = result.Message.Key,
                        Timestamp = result.Message.Timestamp,
                        Value = typeof(T) == typeof(string) ? (dynamic) result.Message.Value : this._converter.JsonToObject<T>(result.Message.Value)
                    };
                    results.Add(message);
                }

                return results;
            }
        }

        private string GetKeyProducerListener(string hostPort, string topicName, string pubSubName = null) {
            return !string.IsNullOrEmpty(pubSubName) ? pubSubName : $"KAFKA_PRODUCER_{hostPort.ToUpper()}#{topicName.ToUpper()}";
        }

        private async Task<string> GetTopicNameProducerListener(string topicName, bool suffixKodeDc = false) {
            if (suffixKodeDc) {
                if (!topicName.EndsWith("_")) {
                    topicName += "_";
                }

                string kodeDc = await this._db.GetKodeDc();
                topicName += kodeDc;
            }

            return topicName;
        }

        public async void CreateKafkaProducerListener(string hostPort, string topicName, short replication = -1, int partition = -1, bool suffixKodeDc = false, CancellationToken stoppingToken = default, string pubSubName = null) {
            topicName = await this.GetTopicNameProducerListener(topicName, suffixKodeDc);
            await this.CreateTopicIfNotExist(hostPort, topicName, replication, partition);
            string key = this.GetKeyProducerListener(hostPort, topicName, pubSubName);

            if (!this.keyValuePairs.ContainsKey(key)) {
                this.keyValuePairs.Add(key, this.CreateKafkaProducerInstance<string, string>(hostPort));
            }

            this._pubSub.GetGlobalAppBehaviorSubject<KafkaMessage<string, dynamic>>(key).Subscribe(async data => {
                if (data != null) {
                    var msg = new Message<string, string> {
                        Headers = this.CreateHeaderFromDictionary(data.Headers),
                        Key = data.Key,
                        Timestamp = data.Timestamp,
                        Value = typeof(string) == data.Value.GetType() ? data.Value : this._converter.ObjectToJson(data.Value)
                    };
                    await this.keyValuePairs[key].ProduceAsync(topicName, msg, stoppingToken);
                }
            });
        }

        public async void DisposeAndRemoveKafkaProducerListener(string hostPort, string topicName, bool suffixKodeDc = false, string pubSubName = null) {
            topicName = await this.GetTopicNameProducerListener(topicName, suffixKodeDc);
            string key = this.GetKeyProducerListener(hostPort, topicName, pubSubName);

            if (this.keyValuePairs.ContainsKey(key)) {
                this.keyValuePairs[key].Dispose();
                this.keyValuePairs.Remove(key);
            }

            this._pubSub.DisposeAndRemoveSubscriber(key);
        }

        private async Task<(string, string)> GetTopicNameConsumerListener(string topicName, string groupId, bool suffixKodeDc = false) {
            if (suffixKodeDc) {
                if (!groupId.EndsWith("_")) {
                    groupId += "_";
                }

                if (!topicName.EndsWith("_")) {
                    topicName += "_";
                }

                string kodeDc = await this._db.GetKodeDc();
                groupId += kodeDc;
                topicName += kodeDc;
            }

            return (topicName, groupId);
        }

        public async void CreateKafkaConsumerListener<T>(string hostPort, string topicName, string groupId, short replication = -1, int partition = -1, bool suffixKodeDc = false, CancellationToken stoppingToken = default, Action<KafkaMessage<string, T>> execLambda = null, string pubSubName = null) {
            const ulong COMMIT_AFTER_N_MESSAGES = 10;
            (topicName, groupId) = await this.GetTopicNameConsumerListener(topicName, groupId, suffixKodeDc);
            await this.CreateTopicIfNotExist(hostPort, topicName, replication, partition);
            string key = !string.IsNullOrEmpty(pubSubName) ? pubSubName : $"KAFKA_CONSUMER_{hostPort.ToUpper()}#{topicName.ToUpper()}";
            using (IConsumer<string, string> consumer = this.CreateKafkaConsumerInstance<string, string>(hostPort, groupId)) {
                TopicPartition topicPartition = this.CreateKafkaConsumerTopicPartition(topicName, -1);
                TopicPartitionOffset topicPartitionOffset = this.CreateKafkaConsumerTopicPartitionOffset(topicPartition, 0);
                consumer.Assign(topicPartitionOffset);
                consumer.Subscribe(topicName);
                ulong i = 0;
                while (!stoppingToken.IsCancellationRequested) {
                    ConsumeResult<string, string> result = consumer.Consume(stoppingToken);
                    IDictionary<string, string> resMsgHdr = this.MessageToDictionary<string>(result.Message.Headers);
                    try {
                        var msg = new KafkaMessage<string, string> {
                            Headers = resMsgHdr,
                            Key = result.Message.Key,
                            Timestamp = result.Message.Timestamp,
                            Value = result.Message.Value
                        };
                        await this._db.SaveKafkaToTable(result.Topic, result.Offset.Value, result.Partition.Value, msg);
                    }
                    catch (Exception e) {
                        this._logger.WriteError(e);
                    }

                    var message = new KafkaMessage<string, T> {
                        Headers = resMsgHdr,
                        Key = result.Message.Key,
                        Timestamp = result.Message.Timestamp,
                        Value = typeof(T) == typeof(string) ? (dynamic) result.Message.Value : this._converter.JsonToObject<T>(result.Message.Value)
                    };
                    execLambda?.Invoke(message);
                    this._pubSub.GetGlobalAppBehaviorSubject<KafkaMessage<string, T>>(key).OnNext(message);
                    if (++i % COMMIT_AFTER_N_MESSAGES == 0) {
                        consumer.Commit();
                        i = 0;
                    }
                }

                this._pubSub.DisposeAndRemoveSubscriber(key);
            }
        }

        public Dictionary<string, T> MessageToDictionary<T>(object obj) {
            return obj.GetType()
                .GetProperties(BindingFlags.Instance | BindingFlags.Public)
                .ToDictionary(prop => prop.Name, prop => {
                    try {
                        dynamic data = prop.GetValue(obj, null);
                        if (typeof(T) == typeof(string)) {
                            if (data.GetType() == typeof(DateTime)) {
                                data = ((DateTime) data).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ");
                            }

                            if (typeof(T) == typeof(object)) {
                                data = this._converter.ObjectToJson(data);
                            }
                            else {
                                data = $"{data}";
                            }
                        }

                        return (T) data;
                    }
                    catch {
                        return default;
                    }
                });
        }

    }

}
