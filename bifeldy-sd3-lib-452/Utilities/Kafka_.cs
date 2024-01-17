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
using System.Threading.Tasks;

using Confluent.Kafka;

using bifeldy_sd3_lib_452.Models;

namespace bifeldy_sd3_lib_452.Utilities {

    public interface IKafka {
        KafkaProducerConfig GenerateKafkaProducerConfig(string hostPort);
        KafkaProducer<T1, T2> GenerateProducerBuilder<T1, T2>(KafkaProducerConfig config);
        KafkaProducer<T1, T2> CreateKafkaProducerInstance<T1, T2>(string hostPort);
        Task<KafkaDeliveryResult<string, string>> ProduceSingleMessage(string hostPort, string topic, KafkaMessage<string, dynamic> data);
        KafkaConsumerConfig GenerateKafkaConsumerConfig(string hostPort, string groupId, AutoOffsetReset autoOffsetReset);
        KafkaConsumer<T1, T2> GenerateConsumerBuilder<T1, T2>(KafkaConsumerConfig config);
        KafkaConsumer<T1, T2> CreateKafkaConsumerInstance<T1, T2>(string hostPort, string groupId);
        KafkaTopicPartition CreateKafkaConsumerTopicPartition(string topicName, int partition);
        KafkaTopicPartitionOffset CreateKafkaConsumerTopicPartitionOffset(KafkaTopicPartition topicPartition, long offset);
        KafkaMessage<string, dynamic> ConsumeSingleMessage<T>(string hostPort, string groupId, string topicName, int partition = 0, long offset = -1);
    }

    public sealed class CKafka : IKafka {

        private readonly ILogger _logger;
        private readonly IConverter _converter;

        public CKafka(ILogger logger, IConverter converter) {
            _logger = logger;
            _converter = converter;
        }

        public KafkaProducerConfig GenerateKafkaProducerConfig(string hostPort) {
            return new KafkaProducerConfig {
                BootstrapServers = hostPort
            };
        }

        public KafkaProducer<T1, T2> GenerateProducerBuilder<T1, T2>(KafkaProducerConfig config) {
            return (KafkaProducer<T1, T2>) new ProducerBuilder<T1, T2>(config).Build();
        }

        public KafkaProducer<T1, T2> CreateKafkaProducerInstance<T1, T2>(string hostPort) {
            return GenerateProducerBuilder<T1, T2>(GenerateKafkaProducerConfig(hostPort));
        }

        public async Task<KafkaDeliveryResult<string, string>> ProduceSingleMessage(string hostPort, string topic, KafkaMessage<string, dynamic> data) {
            using (KafkaProducer<string, string> producer = CreateKafkaProducerInstance<string, string>(hostPort)) {
                Message<string, string> msg = new Message<string, string> {
                    Key = data.Key,
                    Value = typeof(string) == data.Value.GetType() ? data.Value : _converter.ObjectToJson(data.Value)
                };
                return (KafkaDeliveryResult<string, string>) await producer.ProduceAsync(topic, msg);
            }
        }

        public KafkaConsumerConfig GenerateKafkaConsumerConfig(string hostPort, string groupId, AutoOffsetReset autoOffsetReset) {
            return new KafkaConsumerConfig {
                BootstrapServers = hostPort,
                GroupId = groupId,
                AutoOffsetReset = autoOffsetReset,
                EnableAutoCommit = false
            };
        }

        public KafkaConsumer<T1, T2> GenerateConsumerBuilder<T1, T2>(KafkaConsumerConfig config) {
            return (KafkaConsumer<T1, T2>) new ConsumerBuilder<T1, T2>(config).Build();
        }

        public KafkaConsumer<T1, T2> CreateKafkaConsumerInstance<T1, T2>(string hostPort, string groupId) {
            return GenerateConsumerBuilder<T1, T2>(GenerateKafkaConsumerConfig(hostPort, groupId, AutoOffsetReset.Earliest));
        }

        public KafkaTopicPartition CreateKafkaConsumerTopicPartition(string topicName, int partition) {
            return new KafkaTopicPartition(topicName, Math.Max(Partition.Any, partition));
        }

        public KafkaTopicPartitionOffset CreateKafkaConsumerTopicPartitionOffset(KafkaTopicPartition topicPartition, long offset) {
            return new KafkaTopicPartitionOffset(topicPartition, new Offset(offset));
        }

        public KafkaMessage<string, dynamic> ConsumeSingleMessage<T>(string hostPort, string groupId, string topicName, int partition = 0, long offset = -1) {
            using (KafkaConsumer<string, dynamic> consumer = CreateKafkaConsumerInstance<string, dynamic>(hostPort, groupId)) {
                TimeSpan timeout = TimeSpan.FromSeconds(3);
                KafkaTopicPartition topicPartition = CreateKafkaConsumerTopicPartition(topicName, partition);
                if (offset < 0) {
                    WatermarkOffsets watermarkOffsets = consumer.QueryWatermarkOffsets(topicPartition, timeout);
                    offset = watermarkOffsets.High.Value - 1;
                }
                KafkaTopicPartitionOffset topicPartitionOffset = CreateKafkaConsumerTopicPartitionOffset(topicPartition, offset);
                consumer.Assign(topicPartitionOffset);
                ConsumeResult<string, dynamic> result = consumer.Consume(timeout);
                _logger.WriteInfo(GetType().Name, $"[KAFKA_CONSUMER_{consumer.Position(topicPartition)}] 🏗 {result.Message.Key} :: {result.Message.Value}");
                Message<string, dynamic> message = result.Message;
                if (result.Message.Value.StartsWith("{")) {
                    message.Value = _converter.JsonToObject<T>(result.Message.Value);
                }
                consumer.Close();
                return (KafkaMessage<string, dynamic>) message;
            }
        }

    }

}
