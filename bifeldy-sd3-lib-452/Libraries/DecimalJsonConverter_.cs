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
using System.Globalization;

using Newtonsoft.Json;

using bifeldy_sd3_lib_452.Extensions;

namespace bifeldy_sd3_lib_452.Libraries {

    public sealed class DecimalNewtonsoftJsonConverter : JsonConverter<decimal> {

        public override decimal ReadJson(
            JsonReader reader,
            Type objectType,
            decimal existingValue,
            bool hasExistingValue,
            JsonSerializer serializer
        ) {
            if (
                reader.TokenType == JsonToken.Integer ||
                reader.TokenType == JsonToken.Float
            ) {
                return Convert.ToDecimal(reader.Value, CultureInfo.InvariantCulture).RemoveTrail();
            }

            if (reader.TokenType == JsonToken.String) {
                string s = (string)reader.Value;

                if (decimal.TryParse(s, NumberStyles.Any, CultureInfo.InvariantCulture, out decimal d)) {
                    return d.RemoveTrail();
                }
            }

            throw new JsonSerializationException($"Unexpected token {reader.TokenType}");
        }

        public override void WriteJson(JsonWriter writer, decimal value, JsonSerializer serializer) {
            writer.WriteRawValue(value.RemoveTrail().ToString(CultureInfo.InvariantCulture));
        }

    }


    public sealed class NullableDecimalNewtonsoftJsonConverter : JsonConverter<decimal?> {

        public override decimal? ReadJson(
            JsonReader reader,
            Type objectType,
            decimal? existingValue,
            bool hasExistingValue,
            JsonSerializer serializer
        ) {
            if (reader.TokenType == JsonToken.Null) {
                return null;
            }

            if (
                reader.TokenType == JsonToken.Integer ||
                reader.TokenType == JsonToken.Float
            ) {
                return Convert.ToDecimal(reader.Value, CultureInfo.InvariantCulture).RemoveTrail();
            }

            if (reader.TokenType == JsonToken.String) {
                string s = (string)reader.Value;

                if (decimal.TryParse(s, NumberStyles.Any, CultureInfo.InvariantCulture, out decimal d)) {
                    return d.RemoveTrail();
                }

                return null;
            }

            throw new JsonSerializationException($"Unexpected token {reader.TokenType}");
        }

        public override void WriteJson(JsonWriter writer, decimal? value, JsonSerializer serializer) {
            if (value == null) {
                writer.WriteNull();
            }
            else {
                writer.WriteRawValue(value.Value.RemoveTrail().ToString(CultureInfo.InvariantCulture));
            }
        }

    }

}
