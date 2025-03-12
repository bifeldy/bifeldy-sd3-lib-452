/**
 * 
 * Author       :: Basilius Bias Astho Christyono
 * Phone        :: (+62) 889 236 6466
 * 
 * Department   :: IT SD 03
 * Mail         :: bias@indomaret.co.id
 * 
 * Catatan      :: Pengaturan Aplikasi
 *              :: Harap Didaftarkan Ke DI Container
 * 
 */

using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.NetworkInformation;
using System.Reflection;
using System.Threading;
using System.Windows;

using bifeldy_sd3_lib_452.Models;

namespace bifeldy_sd3_lib_452.Utilities {

    public interface IApplication {
        void Shutdown();
        Process CurrentProcess { get; }
        bool DebugMode { get; set; }
        bool IsIdle { get; set; }
        bool IsSkipUpdate { get; }
        string AppPath { get; }
        string AppName { get; }
        string AppLocation { get; }
        string AppVersion { get; }
        string GetConfig(string key);
        string SettingLibMultiGetThreadName(int id);
        int SettingLibMultiGetJumlahKoneksi();
        string SettingLibMultiGetListKey(int idx);
        string GetVariabel(string key);
        CIpMacAddress[] GetIpMacAddress();
        string[] GetAllIpAddress();
        string[] GetAllMacAddress();
        bool IsUsingPostgres { get; set; }
    }

    public class CApplication : IApplication {

        private readonly IConfig _config;

        public Process CurrentProcess { get; }
        public bool DebugMode { get; set; } = false;
        public bool IsIdle { get; set; } = false;
        public bool IsSkipUpdate { get; } = false;

        private readonly SettingLib.Class1 _SettingLib;
        private readonly SettingLibRest.Class1 _SettingLibRest;
        private readonly SettingLibMulti.Class1 _SettingLibMulti;

        public string AppPath { get; }
        public string AppName { get; }
        public string AppLocation { get; }
        public string AppVersion { get; }

        public bool IsUsingPostgres { get; set; }

        private AppSettingsSection AppSettings = null;

        public static string _SETTING_LIB_MULTI_THREAD_NAME = "THREAD#";

        public CApplication(IConfig config) {
            this._config = config;

            this.CurrentProcess = Process.GetCurrentProcess();
            string[] args = Environment.GetCommandLineArgs();
            for (int i = 0; i < args.Length; i++) {
                if (args[i].ToUpper() == "--SKIP-UPDATE") {
                    this.IsSkipUpdate = true;
                }
            }

            #if DEBUG
                this.DebugMode = true;
            #endif

            this._SettingLib = new SettingLib.Class1();
            this._SettingLibRest = new SettingLibRest.Class1();
            this._SettingLibMulti = new SettingLibMulti.Class1();

            this.AppPath = Process.GetCurrentProcess().MainModule.FileName;
            string appName = Process.GetCurrentProcess().MainModule.ModuleName.ToUpper();
            this.AppName = appName.Substring(0, appName.LastIndexOf(".EXE"));
            this.AppLocation = AppDomain.CurrentDomain.BaseDirectory;
            this.AppVersion = string.Join("", Process.GetCurrentProcess().MainModule.FileVersionInfo.FileVersion.Split('.'));
        }

        public void Shutdown() => Application.Current.Shutdown();

        public string GetConfig(string key) {
            try {
                // App.config -- Build Action: Embedded Resource
                if (this.AppSettings == null) {
                    var asm = Assembly.LoadFile(this.AppPath);
                    string ns = asm.GetManifestResourceNames().FirstOrDefault(n => n.EndsWith(".App.config"));
                    using (Stream strm = asm.GetManifestResourceStream(ns)) {
                        using (var sr = new StreamReader(strm)) {
                            File.WriteAllText($"{this.AppPath}.config", sr.ReadToEnd());
                            this.AppSettings = ConfigurationManager.OpenExeConfiguration(this.AppPath).AppSettings;
                            File.Delete($"{this.AppPath}.config");
                        }
                    }
                }

                return this.AppSettings.Settings[key].Value;
            }
            catch {
                return null;
            }
        }

        public string SettingLibMultiGetThreadName(int id) {
            return $"{_SETTING_LIB_MULTI_THREAD_NAME}{id}";
        }

        public int SettingLibMultiGetJumlahKoneksi() {
            return this._SettingLibMulti.GetJmlKoneksi();
        }

        public string SettingLibMultiGetListKey(int idx) {
            return this._SettingLibMulti.GetListKey(idx);
        }

        public string GetVariabel(string key) {
            string id = string.Empty;
            if (this.DebugMode) {
                id = this._config.Get<string>("SettingDebug", this.GetConfig("setting_debug"));
            }
            
            bool localDbOnly = this._config.Get<bool>("LocalDbOnly", bool.Parse(this.GetConfig("local_db_only")));
            if (localDbOnly) {
                return null;
            }

            int idxMulti = -1;

            // _SETTING_LIB_MULTI_#0
            string threadName = Thread.CurrentThread.Name?.ToUpper();
            if (!string.IsNullOrEmpty(threadName)) {
                if (threadName.StartsWith(_SETTING_LIB_MULTI_THREAD_NAME)) {
                    idxMulti = int.Parse(threadName.Split('#').Last());
                }
            }

            string result = null;

            if (idxMulti < 0) {
                try {
                    // http://xxx.xxx.xxx.xxx/KunciGxxx/Service.asmx
                    result = this._SettingLib.GetVariabel(key, id);
                    if (result.ToUpper().Contains("ERROR")) {
                        throw new Exception("SettingLib Gagal");
                    }
                }
                catch {
                    try {
                        // http://xxx.xxx.xxx.xxx/KunciGxxx
                        result = this._SettingLibRest.GetVariabel(key);
                        if (result.ToUpper().Contains("ERROR")) {
                            throw new Exception("SettingLibRest Gagal");
                        }
                    }
                    catch {
                        result = null;
                    }
                }
            }
            else {
                try {
                    // GXXX=http://xxx.xxx.xxx.xxx/KunciGxxx/Service.asmx
                    // GXXXSIM=http://xxx.xxx.xxx.xxx/KunciGxxxSim/Service.asmx
                    string idx = this.SettingLibMultiGetListKey(idxMulti);
                    result = this._SettingLibMulti.GetVariabel(key, idx);
                    if (result.ToUpper().Contains("ERROR")) {
                        throw new Exception("SettingLibMulti Gagal");
                    }
                }
                catch {
                    result = null;
                }
            }

            if (!string.IsNullOrEmpty(result)) {
                result = result.Split(';').FirstOrDefault();
            }

            return result;
        }

        public CIpMacAddress[] GetIpMacAddress() {
            var IpMacAddress = new List<CIpMacAddress>();
            NetworkInterface[] nics = NetworkInterface.GetAllNetworkInterfaces();
            foreach (NetworkInterface nic in nics) {
                if (nic.OperationalStatus == OperationalStatus.Up && nic.NetworkInterfaceType != NetworkInterfaceType.Loopback) {
                    PhysicalAddress mac = nic.GetPhysicalAddress();
                    string iv4 = null;
                    string iv6 = null;
                    IPInterfaceProperties ipInterface = nic.GetIPProperties();
                    foreach (UnicastIPAddressInformation ua in ipInterface.UnicastAddresses) {
                        if (!ua.Address.IsIPv4MappedToIPv6 && !ua.Address.IsIPv6LinkLocal && !ua.Address.IsIPv6Teredo && !ua.Address.IsIPv6SiteLocal) {
                            if (ua.PrefixLength <= 32) {
                                iv4 = ua.Address.ToString();
                            }
                            else if (ua.PrefixLength <= 64) {
                                iv6 = ua.Address.ToString();
                            }
                        }
                    }

                    IpMacAddress.Add(new CIpMacAddress {
                        NAME = nic.Name,
                        DESCRIPTION = nic.Description,
                        MAC_ADDRESS = string.IsNullOrEmpty(mac.ToString()) ? null : mac.ToString(),
                        IP_V4_ADDRESS = iv4,
                        IP_V6_ADDRESS = iv6
                    });
                }
            }

            return IpMacAddress.ToArray();
        }

        public string[] GetAllIpAddress() {
            string[] iv4 = this.GetIpMacAddress().Where(d => !string.IsNullOrEmpty(d.IP_V4_ADDRESS)).Select(d => d.IP_V4_ADDRESS.ToUpper()).ToArray();
            string[] iv6 = this.GetIpMacAddress().Where(d => !string.IsNullOrEmpty(d.IP_V6_ADDRESS)).Select(d => d.IP_V6_ADDRESS.ToUpper()).ToArray();
            string[] ip = new string[iv4.Length + iv6.Length];
            iv4.CopyTo(ip, 0);
            iv6.CopyTo(ip, iv4.Length);
            return ip;
        }

        public string[] GetAllMacAddress() {
            return this.GetIpMacAddress().Where(d => !string.IsNullOrEmpty(d.MAC_ADDRESS)).Select(d => d.MAC_ADDRESS.ToUpper()).ToArray();
        }

    }

}
