/**
 * 
 * Author       :: Basilius Bias Astho Christyono
 * Phone        :: (+62) 889 236 6466
 * 
 * Department   :: IT SD 03
 * Mail         :: bias@indomaret.co.id
 * 
 * Catatan      :: Tidak Untuk Didaftarkan Ke DI Container
 * 
 */

using System;

using bifeldy_sd3_lib_452.Extensions;

namespace bifeldy_sd3_lib_452.Libraries {

    public sealed class DecimalNewtonsoftJsonConverter : Newtonsoft.Json.JsonConverter<decimal> {

        public override decimal ReadJson(
            Newtonsoft.Json.JsonReader reader,
            Type objectType,
            decimal existingValue,
            bool hasExistingValue,
            Newtonsoft.Json.JsonSerializer serializer
        ) {
            decimal? _val = reader.ReadAsDecimal();

            decimal val = 0;
            if (_val.HasValue) {
                val = _val.Value;
            }

            return val.RemoveTrail();
        }

        public override void WriteJson(
            Newtonsoft.Json.JsonWriter writer,
            decimal value,
            Newtonsoft.Json.JsonSerializer serializer
        ) {
            writer.WriteRawValue(value.ToString(true));
        }

    }

}
