/**
* 
* Author       :: Basilius Bias Astho Christyono
* Phone        :: (+62) 889 236 6466
* 
* Department   :: IT SD 03
* Mail         :: bias@indomaret.co.id
* 
* Catatan      :: Template Kafka Message
*              :: Model Supaya Tidak Perlu Install Package Nuget Kafka
* 
*/

using Confluent.Kafka;

namespace bifeldy_sd3_lib_452.Models {

    public sealed class KafkaMessage<TKey, TValue> : Message<TKey, TValue> {
        public new Headers Headers { get; set; }
        public new TKey Key { get; set; }
        public new TValue Value { get; set; }
        public new Timestamp Timestamp { get; set; }
    }

    public sealed class KafkaDeliveryResult<TKey, TValue> : DeliveryResult<TKey, TValue> {
        public new Headers Headers { get; set; }
        public new string Key { get; set; }
        public new Message<string, string> Message { get; set; }
        public new Offset Offset { get; set; }
        public new Partition Partition { get; set; }
        public new PersistenceStatus Status { get; set; }
        public new Timestamp Timestamp { get; set; }
        public new string Topic { get; set; }
        public new TopicPartition TopicPartition { get; set; }
    }

}
