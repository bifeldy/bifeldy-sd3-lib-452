/**
 * 
 * Author       :: Basilius Bias Astho Christyono
 * Phone        :: (+62) 889 236 6466
 * 
 * Department   :: IT SD 03
 * Mail         :: bias@indomaret.co.id
 * 
 * Catatan      :: Alat Konversi
 *              :: Harap Didaftarkan Ke DI Container
 * 
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Drawing;
using System.Reflection;

using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

using bifeldy_sd3_lib_452.Libraries;

namespace bifeldy_sd3_lib_452.Utilities {

    public interface IConverter {
        byte[] ImageToByte(Image x);
        Image ByteToImage(byte[] byteArray);
        T JsonToObject<T>(string j2o);
        T ObjectToT<T>(object o2t);
        string ObjectToJson(object body);
        string FormatByteSizeHumanReadable(long bytes, string forceUnit = null);
    }

    public sealed class CConverter : IConverter {

        public CConverter() {
            //
        }

        public byte[] ImageToByte(Image image) {
            return (byte[]) new ImageConverter().ConvertTo(image, typeof(byte[]));
        }

        public Image ByteToImage(byte[] byteArray) {
            return (Bitmap) new ImageConverter().ConvertFrom(byteArray);
        }

        private List<object> JArrayToList(JArray jsonArray) {
            var result = new List<object>();

            foreach (JToken item in jsonArray) {
                switch (item.Type) {
                    case JTokenType.Object:
                        result.Add(this.JObjectToDictionary((JObject)item));
                        break;
                    case JTokenType.Array:
                        result.Add(this.JArrayToList((JArray)item));
                        break;
                    default:
                        result.Add(item.ToObject<object>());
                        break;
                }
            }

            return result;
        }

        private Dictionary<string, object> JObjectToDictionary(JObject jsonObject) {
            var result = new Dictionary<string, object>();

            foreach (JProperty property in jsonObject.Properties()) {
                string key = property.Name;
                JToken value = property.Value;

                switch (value.Type) {
                    case JTokenType.Object:
                        result[key] = this.JObjectToDictionary((JObject)value);
                        break;
                    case JTokenType.Array:
                        result[key] = this.JArrayToList((JArray)value);
                        break;
                    default:
                        result[key] = value.ToObject<object>();
                        break;
                }
            }

            return result;
        }

        public T JsonToObject<T>(string j2o) {
            if (typeof(IDictionary<string, object>).IsAssignableFrom(typeof(T))) {
                var jObject = JObject.Parse(j2o);
                return (dynamic)this.JObjectToDictionary(jObject);
            }

            return JsonConvert.DeserializeObject<T>(j2o, new JsonSerializerSettings {
                Converters = new JsonConverter[] {
                    new DecimalNewtonsoftJsonConverter(),
                    new NullableDecimalNewtonsoftJsonConverter()
                }
            });
        }

        public string ObjectToJson(object o2j) {
            return JsonConvert.SerializeObject(o2j, new JsonSerializerSettings {
                Converters = new JsonConverter[] {
                    new DecimalNewtonsoftJsonConverter(),
                    new NullableDecimalNewtonsoftJsonConverter()
                }
            });
        }

        public T ObjectToT<T>(object o2t) {
            TypeConverter converter = TypeDescriptor.GetConverter(typeof(T));
            if (converter.CanConvertFrom(o2t.GetType())) {
                return (T) converter.ConvertFrom(o2t);
            }
            else {
                return (T) Convert.ChangeType(o2t, typeof(T));
            }
        }

        public string FormatByteSizeHumanReadable(long bytes, string forceUnit = null) {
            IDictionary<string, long> dict = new Dictionary<string, long> {
                { "TB", 1000000000000 },
                { "GB", 1000000000 },
                { "MB", 1000000 },
                { "KB", 1000 },
                { "B", 1 }
            };
            long digit = 1;
            string ext = "B";
            if (!string.IsNullOrEmpty(forceUnit)) {
                digit = dict[forceUnit];
                ext = forceUnit;
            }
            else {
                foreach (KeyValuePair<string, long> kvp in dict) {
                    if (bytes > kvp.Value) {
                        digit = kvp.Value;
                        ext = kvp.Key;
                        break;
                    }
                }
            }

            return $"{(decimal) bytes / digit:0.00} {ext}";
        }

        public List<CDynamicClassProperty> GetTableClassStructureModel<T>() {
            var ls = new List<CDynamicClassProperty>();

            foreach (PropertyInfo propertyInfo in typeof(T).GetProperties()) {
                Type type = Nullable.GetUnderlyingType(propertyInfo.PropertyType);
                ls.Add(new CDynamicClassProperty() {
                    ColumnName = propertyInfo.Name,
                    DataType = type?.FullName ?? propertyInfo.PropertyType.FullName,
                    IsNullable = type != null
                });
            }

            return ls;
        }

    }

}
