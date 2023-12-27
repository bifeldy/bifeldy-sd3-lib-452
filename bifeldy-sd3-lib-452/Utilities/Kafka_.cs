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
using bifeldy_sd3_lib_452.Utilities;

namespace bifeldy_sd3_lib_452.Services {

    public interface IKafkaService {
        ProducerConfig GenerateProducerConfig(string hostPort);
        IProducer<T1, T2> GenerateProducerBuilder<T1, T2>(ProducerConfig config);
        IProducer<T1, T2> CreateKafkaProducerInstance<T1, T2>(string hostPort);
        Task<DeliveryResult<string, string>> ProduceSingleMessage(string hostPort, string topic, KafkaMessage<string, dynamic> data);
        ConsumerConfig GenerateConsumerConfig(string hostPort, string groupId, AutoOffsetReset autoOffsetReset);
        IConsumer<T1, T2> GenerateConsumerBuilder<T1, T2>(ConsumerConfig config);
        IConsumer<T1, T2> CreateKafkaConsumerInstance<T1, T2>(string hostPort, string groupId);
        TopicPartition CreateKafkaConsumerTopicPartition(string topicName, int partition);
        TopicPartitionOffset CreateKafkaConsumerTopicPartitionOffset(TopicPartition topicPartition, long offset);
        KafkaMessage<string, dynamic> ConsumeSingleMessage<T>(string hostPort, string groupId, string topicName, int partition = 0, long offset = -1);
    }

    public sealed class CKafkaService : IKafkaService {

        private readonly ILogger _logger;
        private readonly IConverter _converter;

        public CKafkaService(ILogger logger, IConverter converter) {
            _logger = logger;
            _converter = converter;
        }

        public ProducerConfig GenerateProducerConfig(string hostPort) {
            return new ProducerConfig {
                BootstrapServers = hostPort
            };
        }

        public IProducer<T1, T2> GenerateProducerBuilder<T1, T2>(ProducerConfig config) {
            return new ProducerBuilder<T1, T2>(config).Build();
        }

        public IProducer<T1, T2> CreateKafkaProducerInstance<T1, T2>(string hostPort) {
            return GenerateProducerBuilder<T1, T2>(GenerateProducerConfig(hostPort));
        }

        public async Task<DeliveryResult<string, string>> ProduceSingleMessage(string hostPort, string topic, KafkaMessage<string, dynamic> data) {
            using (IProducer<string, string> producer = CreateKafkaProducerInstance<string, string>(hostPort)) {
                Message<string, string> msg = new Message<string, string> {
                    Key = data.Key,
                    Value = typeof(string) == data.Value.GetType() ? data.Value : _converter.ObjectToJson(data.Value)
                };
                return await producer.ProduceAsync(topic, msg);
            }
        }

        public ConsumerConfig GenerateConsumerConfig(string hostPort, string groupId, AutoOffsetReset autoOffsetReset) {
            return new ConsumerConfig {
                BootstrapServers = hostPort,
                GroupId = groupId,
                AutoOffsetReset = autoOffsetReset,
                EnableAutoCommit = false
            };
        }

        public IConsumer<T1, T2> GenerateConsumerBuilder<T1, T2>(ConsumerConfig config) {
            return new ConsumerBuilder<T1, T2>(config).Build();
        }

        public IConsumer<T1, T2> CreateKafkaConsumerInstance<T1, T2>(string hostPort, string groupId) {
            return GenerateConsumerBuilder<T1, T2>(GenerateConsumerConfig(hostPort, groupId, AutoOffsetReset.Earliest));
        }

        public TopicPartition CreateKafkaConsumerTopicPartition(string topicName, int partition) {
            return new TopicPartition(topicName, Math.Max(Partition.Any, partition));
        }

        public TopicPartitionOffset CreateKafkaConsumerTopicPartitionOffset(TopicPartition topicPartition, long offset) {
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
                _logger.WriteInfo(GetType().Name, $"[KAFKA_CONSUMER_{consumer.Position(topicPartition)}] 🏗 {result.Message.Key} :: {result.Message.Value}");
                Message<string, dynamic> message = result.Message;
                if (result.Message.Value.StartsWith("{")) {
                    message.Value = _converter.JsonToObject<T>(result.Message.Value);
                }
                consumer.Close();
                return (KafkaMessage<string, dynamic>)message;
            }
        }

    }

}
