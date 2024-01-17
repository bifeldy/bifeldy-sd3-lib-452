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
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka;

using bifeldy_sd3_lib_452.Handlers;
using bifeldy_sd3_lib_452.Models;

namespace bifeldy_sd3_lib_452.Utilities {

    public interface IKafka {
        Task<KafkaDeliveryResult<string, string>> ProduceSingleMessage(string hostPort, string topic, KafkaMessage<string, dynamic> data);
        KafkaMessage<string, dynamic> ConsumeSingleMessage<T>(string hostPort, string groupId, string topicName, int partition = 0, long offset = -1);
        Task CreateKafkaProducerListener(string hostPort, string topicName, bool suffixKodeDc = false, CancellationToken stoppingToken = default);
        void DisposeAndRemoveKafkaProducerListener(string hostPort, string topicName, bool suffixKodeDc = false);
        Task CreateKafkaConsumerListener(string hostPort, string topicName, string groupId, bool suffixKodeDc = false, CancellationToken stoppingToken = default, Action<KafkaMessage<string, dynamic>> execLambda = null);
    }

    public sealed class CKafka : IKafka {

        private readonly ILogger _logger;
        private readonly IConverter _converter;
        private readonly IPubSub _pubSub;
        private readonly IDbHandler _db;

        public CKafka(ILogger logger, IConverter converter, IPubSub pubSub, IDbHandler db) {
            _logger = logger;
            _converter = converter;
            _pubSub = pubSub;
            _db = db;
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

        public async Task<KafkaDeliveryResult<string, string>> ProduceSingleMessage(string hostPort, string topic, KafkaMessage<string, dynamic> data) {
            using (IProducer<string, string> producer = CreateKafkaProducerInstance<string, string>(hostPort)) {
                if (typeof(string) != data.Value.GetType()) {
                    data.Value = _converter.ObjectToJson(data.Value);
                }
                string jsonText1 = _converter.ObjectToJson(data);
                KafkaMessage<string, string> jsonObj1 = _converter.JsonToObject<KafkaMessage<string, string>>(jsonText1);
                DeliveryResult<string, string> res = await producer.ProduceAsync(topic, jsonObj1);
                string jsonText2 = _converter.ObjectToJson(res);
                _logger.WriteInfo($"{GetType().Name}ProduceSingle", jsonText2);
                KafkaDeliveryResult<string, string> jsonObj2 = _converter.JsonToObject<KafkaDeliveryResult<string, string>>(jsonText2);
                return jsonObj2;
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

        public KafkaMessage<string, dynamic> ConsumeSingleMessage<T>(string hostPort, string groupId, string topicName, int partition = 0, long offset = -1) {
            using (IConsumer<string, dynamic> consumer = CreateKafkaConsumerInstance<string, dynamic>(hostPort, groupId)) {
                TimeSpan timeout = TimeSpan.FromSeconds(3);
                TopicPartition topicPartition = CreateKafkaConsumerTopicPartition(topicName, partition);
                if (offset < 0) {
                    WatermarkOffsets watermarkOffsets = consumer.QueryWatermarkOffsets(topicPartition, timeout);
                    offset = watermarkOffsets.High.Value - 1;
                }
                TopicPartitionOffset topicPartitionOffset = CreateKafkaConsumerTopicPartitionOffset(topicPartition, offset);
                consumer.Assign(topicPartitionOffset);
                ConsumeResult<string, dynamic> result = consumer.Consume(timeout);
                Message<string, dynamic> message = result.Message;
                string jsonText = _converter.ObjectToJson(message);
                _logger.WriteInfo($"{GetType().Name}ConsumeSingle", jsonText);
                KafkaMessage<string, dynamic> jsonObj = _converter.JsonToObject<KafkaMessage<string, dynamic>>(jsonText);
                if (jsonObj.Value.StartsWith("{")) {
                    jsonObj.Value = _converter.JsonToObject<T>(jsonObj.Value);
                }
                consumer.Close();
                return jsonObj;
            }
        }

        private string GetKeyProducerListener(string hostPort, string topicName) {
            return $"KAFKA_PRODUCER_{hostPort.ToUpper()}#{topicName.ToUpper()}";
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

        public async Task CreateKafkaProducerListener(string hostPort, string topicName, bool suffixKodeDc = false, CancellationToken stoppingToken = default) {
            topicName = await GetTopicNameProducerListener(topicName, suffixKodeDc);
            string key = GetKeyProducerListener(hostPort, topicName);
            IProducer<string, string> producer = CreateKafkaProducerInstance<string, string>(hostPort);
            _pubSub.GetGlobalAppBehaviorSubject<KafkaMessage<string, dynamic>>(key).Subscribe(async data => {
                if (data != null) {
                    if (typeof(string) != data.Value.GetType()) {
                        data.Value = _converter.ObjectToJson(data.Value);
                    }
                    string jsonText = _converter.ObjectToJson(data);
                    _logger.WriteInfo($"{GetType().Name}ProducerListener", jsonText);
                    KafkaMessage<string, string> jsonObj = _converter.JsonToObject<KafkaMessage<string, string>>(jsonText);
                    await producer.ProduceAsync(topicName, jsonObj, stoppingToken);
                }
            });
        }

        public async void DisposeAndRemoveKafkaProducerListener(string hostPort, string topicName, bool suffixKodeDc = false) {
            topicName = await GetTopicNameProducerListener(topicName, suffixKodeDc);
            string key = GetKeyProducerListener(hostPort, topicName);
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

        public async Task CreateKafkaConsumerListener(string hostPort, string topicName, string groupId, bool suffixKodeDc = false, CancellationToken stoppingToken = default, Action<KafkaMessage<string, dynamic>> execLambda = null) {
            (topicName, groupId) = await GetTopicNameConsumerListener(topicName, groupId, suffixKodeDc);
            string key = $"KAFKA_CONSUMER_{hostPort.ToUpper()}#{topicName.ToUpper()}";
            IConsumer<string, string> consumer = CreateKafkaConsumerInstance<string, string>(hostPort, groupId);
            TopicPartition topicPartition = CreateKafkaConsumerTopicPartition(topicName, -1);
            TopicPartitionOffset topicPartitionOffset = CreateKafkaConsumerTopicPartitionOffset(topicPartition, 0);
            consumer.Assign(topicPartitionOffset);
            consumer.Subscribe(topicName);
            ulong i = 0;
            while (!stoppingToken.IsCancellationRequested) {
                ConsumeResult<string, string> result = consumer.Consume(stoppingToken);
                Message<string, string> message = result.Message;
                string jsonText = _converter.ObjectToJson(message);
                _logger.WriteInfo($"{GetType().Name}ConsumerListener", jsonText);
                KafkaMessage<string, dynamic> jsonObj = _converter.JsonToObject<KafkaMessage<string, dynamic>>(jsonText);
                if (jsonObj.Value.StartsWith("{")) {
                    jsonObj.Value = _converter.JsonToObject<dynamic>(jsonObj.Value);
                }
                if (execLambda != null) {
                    execLambda(jsonObj);
                }
                _pubSub.GetGlobalAppBehaviorSubject<KafkaMessage<string, dynamic>>(key).OnNext(jsonObj);
                if (++i % 10 == 0) {
                    consumer.Commit();
                    i = 0;
                }
            }
            consumer.Close();
            _pubSub.DisposeAndRemoveSubscriber(key);
        }

    }

}
