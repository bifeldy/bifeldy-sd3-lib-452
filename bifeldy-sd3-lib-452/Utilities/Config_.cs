/**
 * 
 * Author       :: Basilius Bias Astho Christyono
 * Phone        :: (+62) 889 236 6466
 * 
 * Department   :: IT SD 03
 * Mail         :: bias@indomaret.co.id
 * 
 * Catatan      :: Pengaturan Config Program
 *              :: Harap Didaftarkan Ke DI Container
 * 
 */

using System;
using System.Collections.Generic;
using System.Dynamic;
using System.IO;

namespace bifeldy_sd3_lib_452.Utilities {

    public interface IConfig {
        T Get<T>(string keyName, dynamic defaultValue = null, bool encrypted = false);
        void Set(string keyName, dynamic value, bool encrypted = false);
    }

    public sealed class CConfig : IConfig {

        private readonly IConverter _converter;
        private readonly IChiper _chiper;

        private readonly string ConfigPath = null;

        private IDictionary<string, dynamic> AppConfig = null;

        public CConfig(IConverter converter, IChiper chiper) {
            this._converter = converter;
            this._chiper = chiper;

            this.ConfigPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "_data", "configuration.json");
            this.Load();
        }

        private void Load() {
            var fi = new FileInfo(this.ConfigPath);
            if (fi.Exists) {
                using (var reader = new StreamReader(this.ConfigPath)) {
                    string fileContents = reader.ReadToEnd();
                    this.AppConfig = this._converter.JsonToObject<Dictionary<string, dynamic>>(fileContents);
                }
            }
            else if (this.AppConfig == null) {
                this.AppConfig = new ExpandoObject();
            }
        }

        private void Save() {
            string json = this._converter.ObjectToJson(this.AppConfig);
            File.WriteAllText(this.ConfigPath, json);
        }

        public T Get<T>(string keyName, dynamic defaultValue = null, bool encrypted = false) {
            this.Load();
            try {
                dynamic value = this.AppConfig[keyName];
                if (value.GetType() == typeof(string) && encrypted) {
                    value = this._chiper.DecryptText((string) value);
                }

                return (T) Convert.ChangeType(value, typeof(T));
            }
            catch {
                this.Set(keyName, defaultValue, encrypted);
                return this.Get<T>(keyName, defaultValue, encrypted);
            }
        }

        public void Set(string keyName, dynamic value, bool encrypted = false) {
            this.AppConfig[keyName] = (value.GetType() == typeof(string) && encrypted) ? this._chiper.EncryptText(value) : value;
            this.Save();
        }

    }

}
