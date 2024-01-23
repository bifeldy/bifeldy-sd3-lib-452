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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka;
using Confluent.Kafka.Admin;

using bifeldy_sd3_lib_452.Handlers;
using bifeldy_sd3_lib_452.Models;

namespace bifeldy_sd3_lib_452.Utilities {

    public interface IKafka {
        Task CreateTopicIfNotExist(string hostPort, string topicName, short replication = 1, int partition = 1);
        Task<List<KafkaDeliveryResult<string, string>>> ProduceSingleMultipleMessages(string hostPort, string topicName, List<KafkaMessage<string, dynamic>> data);
        Task<List<KafkaMessage<string, T>>> ConsumeSingleMultipleMessages<T>(string hostPort, string groupId, string topicName, int partition = 0, long offset = -1, ulong nMessagesBlock = 1);
        void CreateKafkaProducerListener(string hostPort, string topicName, bool suffixKodeDc = false, CancellationToken stoppingToken = default, string pubSubName = null);
        void DisposeAndRemoveKafkaProducerListener(string hostPort, string topicName, bool suffixKodeDc = false, string pubSubName = null);
        void CreateKafkaConsumerListener<T>(string hostPort, string topicName, string groupId, bool suffixKodeDc = false, CancellationToken stoppingToken = default, Action<KafkaMessage<string, T>> execLambda = null, string pubSubName = null);
    }

    public sealed class CKafka : IKafka {

        private readonly ILogger _logger;
        private readonly IConverter _converter;
        private readonly IPubSub _pubSub;
        private readonly IDbHandler _db;

        TimeSpan timeout = TimeSpan.FromSeconds(10);

        public CKafka(ILogger logger, IConverter converter, IPubSub pubSub, IDbHandler db) {
            _logger = logger;
            _converter = converter;
            _pubSub = pubSub;
            _db = db;
        }

        public async Task CreateTopicIfNotExist(string hostPort, string topicName, short replication = 1, int partition = 1) {
            try {
                AdminClientConfig adminConfig = new AdminClientConfig {
                    BootstrapServers = hostPort
                };
                using (IAdminClient adminClient = new AdminClientBuilder(adminConfig).Build()) {
                    Metadata metadata = adminClient.GetMetadata(timeout);
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
                _logger.WriteError(ex);
            }
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
            return GenerateProducerBuilder<T1, T2>(GenerateKafkaProducerConfig(hostPort));
        }

        public async Task<List<KafkaDeliveryResult<string, string>>> ProduceSingleMultipleMessages(string hostPort, string topicName, List<KafkaMessage<string, dynamic>> data) {
            await CreateTopicIfNotExist(hostPort, topicName);
            using (IProducer<string, string> producer = CreateKafkaProducerInstance<string, string>(hostPort)) {
                List<KafkaDeliveryResult<string, string>> results = new List<KafkaDeliveryResult<string, string>>();
                foreach(KafkaMessage<string, dynamic> d in data) {
                    Message<string, string> msg = new Message<string, string> {
                        Key = d.Key,
                        Value = typeof(string) == d.Value.GetType() ? d.Value : _converter.ObjectToJson(d.Value)
                    };
                    DeliveryResult<string, string> result = await producer.ProduceAsync(topicName, msg);
                    results.Add(new KafkaDeliveryResult<string, string> {
                        Headers = result.Headers,
                        Key = result.Key,
                        Message = result.Message,
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
            return GenerateConsumerBuilder<T1, T2>(GenerateKafkaConsumerConfig(hostPort, groupId, AutoOffsetReset.Earliest));
        }

        private TopicPartition CreateKafkaConsumerTopicPartition(string topicName, int partition) {
            return new TopicPartition(topicName, Math.Max(Partition.Any, partition));
        }

        private TopicPartitionOffset CreateKafkaConsumerTopicPartitionOffset(TopicPartition topicPartition, long offset) {
            return new TopicPartitionOffset(topicPartition, new Offset(offset));
        }

        public async Task<List<KafkaMessage<string, T>>> ConsumeSingleMultipleMessages<T>(string hostPort, string groupId, string topicName, int partition = 0, long offset = -1, ulong nMessagesBlock = 1) {
            await CreateTopicIfNotExist(hostPort, topicName);
            using (IConsumer<string, string> consumer = CreateKafkaConsumerInstance<string, string>(hostPort, groupId)) {
                TopicPartition topicPartition = CreateKafkaConsumerTopicPartition(topicName, partition);
                if (offset < 0) {
                    WatermarkOffsets watermarkOffsets = consumer.QueryWatermarkOffsets(topicPartition, timeout);
                    offset = watermarkOffsets.High.Value - 1;
                }
                TopicPartitionOffset topicPartitionOffset = CreateKafkaConsumerTopicPartitionOffset(topicPartition, offset);
                consumer.Assign(topicPartitionOffset);
                List<KafkaMessage<string, T>> results = new List<KafkaMessage<string, T>>();
                for (ulong i = 0; i < nMessagesBlock; i++) {
                    ConsumeResult<string, string> result = consumer.Consume(timeout);
                    KafkaMessage<string, T> message = new KafkaMessage<string, T> {
                        Headers = result.Message.Headers,
                        Key = result.Message.Key,
                        Value = typeof(T) == typeof(string) ? (dynamic) result.Message.Value : _converter.JsonToObject<T>(result.Message.Value),
                        Timestamp = result.Message.Timestamp
                    };
                    results.Add(message);
                }
                consumer.Close();
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
                string kodeDc = await _db.GetKodeDc();
                topicName += kodeDc;
            }
            return topicName;
        }

        public async void CreateKafkaProducerListener(string hostPort, string topicName, bool suffixKodeDc = false, CancellationToken stoppingToken = default, string pubSubName = null) {
            topicName = await GetTopicNameProducerListener(topicName, suffixKodeDc);
            await CreateTopicIfNotExist(hostPort, topicName);
            string key = GetKeyProducerListener(hostPort, topicName, pubSubName);
            IProducer<string, string> producer = CreateKafkaProducerInstance<string, string>(hostPort);
            _pubSub.GetGlobalAppBehaviorSubject<KafkaMessage<string, dynamic>>(key).Subscribe(async data => {
                if (data != null) {
                    Message<string, string> msg = new Message<string, string> {
                        Key = data.Key,
                        Value = typeof(string) == data.Value.GetType() ? data.Value : _converter.ObjectToJson(data.Value)
                    };
                    await producer.ProduceAsync(topicName, msg, stoppingToken);
                }
            });
        }

        public async void DisposeAndRemoveKafkaProducerListener(string hostPort, string topicName, bool suffixKodeDc = false, string pubSubName = null) {
            topicName = await GetTopicNameProducerListener(topicName, suffixKodeDc);
            string key = GetKeyProducerListener(hostPort, topicName, pubSubName);
            _pubSub.DisposeAndRemoveSubscriber(key);
        }

        private async Task<(string, string)> GetTopicNameConsumerListener(string topicName, string groupId, bool suffixKodeDc = false) {
            if (suffixKodeDc) {
                if (!groupId.EndsWith("_")) {
                    groupId += "_";
                }
                if (!topicName.EndsWith("_")) {
                    topicName += "_";
                }
                string kodeDc = await _db.GetKodeDc();
                groupId += kodeDc;
                topicName += kodeDc;
            }
            return (topicName, groupId);
        }

        public async void CreateKafkaConsumerListener<T>(string hostPort, string topicName, string groupId, bool suffixKodeDc = false, CancellationToken stoppingToken = default, Action<KafkaMessage<string, T>> execLambda = null, string pubSubName = null) {
            const ulong COMMIT_AFTER_N_MESSAGES = 10;
            (topicName, groupId) = await GetTopicNameConsumerListener(topicName, groupId, suffixKodeDc);
            await CreateTopicIfNotExist(hostPort, topicName);
            string key = !string.IsNullOrEmpty(pubSubName) ? pubSubName : $"KAFKA_CONSUMER_{hostPort.ToUpper()}#{topicName.ToUpper()}";
            IConsumer<string, string> consumer = CreateKafkaConsumerInstance<string, string>(hostPort, groupId);
            TopicPartition topicPartition = CreateKafkaConsumerTopicPartition(topicName, -1);
            TopicPartitionOffset topicPartitionOffset = CreateKafkaConsumerTopicPartitionOffset(topicPartition, 0);
            consumer.Assign(topicPartitionOffset);
            consumer.Subscribe(topicName);
            ulong i = 0;
            while (!stoppingToken.IsCancellationRequested) {
                ConsumeResult<string, string> result = consumer.Consume(stoppingToken);
                try {
                    KafkaMessage<string, string> msg = new KafkaMessage<string, string> {
                        Headers = result.Message.Headers,
                        Key = result.Message.Key,
                        Value = result.Message.Value,
                        Timestamp = result.Message.Timestamp
                    };
                    await _db.SaveKafkaToTable(result.Topic, result.Offset.Value, result.Partition.Value, msg);
                }
                catch (Exception e) {
                    _logger.WriteError(e);
                }
                KafkaMessage<string, T> message = new KafkaMessage<string, T> {
                    Headers = result.Message.Headers,
                    Key = result.Message.Key,
                    Value = typeof(T) == typeof(string) ? (dynamic) result.Message.Value : _converter.JsonToObject<T>(result.Message.Value),
                    Timestamp = result.Message.Timestamp
                };
                if (execLambda != null) {
                    execLambda(message);
                }
                _pubSub.GetGlobalAppBehaviorSubject<KafkaMessage<string, T>>(key).OnNext(message);
                if (++i % COMMIT_AFTER_N_MESSAGES == 0) {
                    consumer.Commit();
                    i = 0;
                }
            }
            consumer.Close();
            _pubSub.DisposeAndRemoveSubscriber(key);
        }

    }

}
