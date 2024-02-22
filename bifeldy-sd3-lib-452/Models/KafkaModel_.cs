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

using System.Collections.Generic;

using Confluent.Kafka;

namespace bifeldy_sd3_lib_452.Models {

    public sealed class KafkaMessage<TKey, TValue> /* : Message<TKey, TValue> */ {
        public /* new Headers */ IDictionary<string, string> Headers {
            get; // base.Headers;
            set; // base.Headers = value;
        }
        public /* new */ TKey Key {
            get; // base.Key;
            set; // base.Key = value;
        }
        public /* new */ TValue Value {
            get; // base.Value;
            set; // base.Value = value;
        }
        public /* new */ Timestamp Timestamp {
            get; // base.Timestamp;
            set; // base.Timestamp = value;
        }
    }

    public sealed class KafkaDeliveryResult<TKey, TValue> /* : DeliveryResult<TKey, TValue> */ {
        public /* new Headers */ IDictionary<string, string> Headers {
            get; // base.Headers;
            set; // base.Headers = value;
        }
        public /* new */ TKey Key {
            get; // base.Key;
            set; // base.Key = value;
        }
        public /* new */ KafkaMessage<TKey, TValue> Message {
            get; // base.Message;
            set; // base.Message = value;
        }
        public /* new */ Offset Offset {
            get; // base.Offset;
            set; // base.Offset = value;
        }
        public /* new */ Partition Partition {
            get; // base.Partition;
            set; // base.Partition = value;
        }
        public /* new */ PersistenceStatus Status {
            get; // base.Status;
            set; // base.Status = value;
        }
        public /* new */ Timestamp Timestamp {
            get; // base.Timestamp;
            set; // base.Timestamp = value;
        }
        public /* new */ string Topic {
            get; // base.Topic;
            set; // base.Topic = value;
        }
        public /* new */ TopicPartition TopicPartition {
            // base readonly
            get; set;
        }
        public /* new */ TopicPartitionOffset TopicPartitionOffset {
            get; // base.TopicPartitionOffset;
            set; // base.TopicPartitionOffset = value;
        }
    }

}
