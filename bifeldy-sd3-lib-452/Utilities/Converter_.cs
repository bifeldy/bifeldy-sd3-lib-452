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
using System.Drawing;
using System.Linq;
using System.Reflection;

using Newtonsoft.Json;

namespace bifeldy_sd3_lib_452.Utilities {

    public interface IConverter {
        byte[] ImageToByte(Image x);
        Image ByteToImage(byte[] byteArray);
        T JsonToObject<T>(string j2o);
        string ObjectToJson(object body);
        string FormatByteSizeHumanReadable(long bytes, string forceUnit = null);
        Dictionary<string, T> ClassToDictionary<T>(object obj);
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

        public T JsonToObject<T>(string j2o) {
            return JsonConvert.DeserializeObject<T>(j2o);
        }

        public string ObjectToJson(object o2j) {
            return JsonConvert.SerializeObject(o2j);
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

        public Dictionary<string, T> ClassToDictionary<T>(object obj) {
            return obj.GetType()
                .GetProperties(BindingFlags.Instance | BindingFlags.Public)
                    .ToDictionary(prop => prop.Name, prop => {
                        try {
                            dynamic data = prop.GetValue(obj, null);
                            if (typeof(T) == typeof(string)) {
                                if (data.GetType() == typeof(DateTime)) {
                                    data = ((DateTime) data).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ");
                                }

                                if (typeof(T) == typeof(object)) {
                                    data = ObjectToJson(data);
                                }
                                else {
                                    data = $"{data}";
                                }
                            }

                            return (T) data;
                        }
                        catch {
                            return default;
                        }
                    });
        }

    }

}
