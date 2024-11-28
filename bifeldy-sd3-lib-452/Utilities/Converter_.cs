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

using System.Collections.Generic;
using System.Drawing;

using Newtonsoft.Json;

using bifeldy_sd3_lib_452.Libraries;

namespace bifeldy_sd3_lib_452.Utilities {

    public interface IConverter {
        byte[] ImageToByte(Image x);
        Image ByteToImage(byte[] byteArray);
        T JsonToObject<T>(string j2o);
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

        public T JsonToObject<T>(string j2o) {
            return JsonConvert.DeserializeObject<T>(j2o, new JsonSerializerSettings {
                Converters = new[] {
                    new DecimalNewtonsoftJsonConverter()
                }
            });
        }

        public string ObjectToJson(object o2j) {
            return JsonConvert.SerializeObject(o2j, new JsonSerializerSettings {
                Converters = new[] {
                    new DecimalNewtonsoftJsonConverter()
                }
            });
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

    }

}
