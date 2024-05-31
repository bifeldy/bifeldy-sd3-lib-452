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

        private string ConfigPath = null;

        private IDictionary<string, dynamic> AppConfig = null;

        public CConfig(IConverter converter, IChiper chiper) {
            _converter = converter;
            _chiper = chiper;

            ConfigPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "_data", "configuration.json");
            Load();
        }

        private void Load() {
            FileInfo fi = new FileInfo(ConfigPath);
            if (fi.Exists) {
                using (StreamReader reader = new StreamReader(ConfigPath)) {
                    string fileContents = reader.ReadToEnd();
                    AppConfig = _converter.JsonToObject<Dictionary<string, dynamic>>(fileContents);
                }
            }
            else if (AppConfig == null) {
                AppConfig = new ExpandoObject();
            }
        }

        private void Save() {
            string json = _converter.ObjectToJson(AppConfig);
            File.WriteAllText(ConfigPath, json);
        }

        public T Get<T>(string keyName, dynamic defaultValue = null, bool encrypted = false) {
            Load();
            try {
                dynamic value = AppConfig[keyName];
                if (value.GetType() == typeof(string) && encrypted) {
                    value = _chiper.DecryptText((string) value);
                }
                return (T) Convert.ChangeType(value, typeof(T));
            }
            catch {
                Set(keyName, defaultValue, encrypted);
                return Get<T>(keyName);
            }
        }

        public void Set(string keyName, dynamic value, bool encrypted = false) {
            AppConfig[keyName] = (value.GetType() == typeof(string) && encrypted) ? _chiper.EncryptText(value) : value;
            Save();
        }

    }

}
