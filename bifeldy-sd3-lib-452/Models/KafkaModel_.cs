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
        //
    }

    public sealed class KafkaDeliveryResult<TKey, TValue> : DeliveryResult<TKey, TValue> {
        //
    }

}
