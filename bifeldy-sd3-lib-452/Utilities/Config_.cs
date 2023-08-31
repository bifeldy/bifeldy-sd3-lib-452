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
using System.Reflection;

namespace bifeldy_sd3_lib_452.Utilities {

    public interface IConfig {
        T Get<T>(string keyName, dynamic defaultValue = null);
        void Set(string keyName, dynamic value);
    }

    public sealed class CConfig : IConfig {

        private readonly IApplication _app;
        private readonly IConverter _converter;

        private string ConfigPath = null;

        private IDictionary<string, dynamic> AppConfig = null;

        public CConfig(IApplication app, IConverter converter) {
            _app = app;
            _converter = converter;

            ConfigPath = Path.Combine(_app.AppLocation, "configuration.json");
            Load();
        }

        private void Load() {
            FileInfo fi = new FileInfo(ConfigPath);
            if (fi.Exists) {
                using (StreamReader reader = new StreamReader(ConfigPath)) {
                    string fileContents = reader.ReadToEnd();
                    reader.Close();
                    reader.Dispose();
                    AppConfig = _converter.JsonToObj<Dictionary<string, dynamic>>(fileContents);
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

        public T Get<T>(string keyName, dynamic defaultValue = null) {
            Load();
            try {
                return (T) Convert.ChangeType(AppConfig[keyName], typeof(T));
            }
            catch {
                Set(keyName, defaultValue);
                return Get<T>(keyName);
            }
        }

        public void Set(string keyName, dynamic value) {
            AppConfig[keyName] = value;
            Save();
        }

    }

}
