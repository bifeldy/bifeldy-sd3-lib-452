/**
 * 
 * Author       :: Basilius Bias Astho Christyono
 * Phone        :: (+62) 889 236 6466
 * 
 * Department   :: IT SD 03
 * Mail         :: bias@indomaret.co.id
 * 
 * Catatan      :: Windows Registry
 *              :: Harap Didaftarkan Ke DI Container
 * 
 */

using System;
using System.Security.AccessControl;

using Microsoft.Win32;

namespace bifeldy_sd3_lib_452.Utilities {

    public interface IWinReg {
        void SetWindowsStartup(bool startAtBoot);
        void ReSetConfigWindowsStartup();
    }

    public sealed class CWinReg : IWinReg {

        private readonly IApplication _app;
        private readonly IConfig _config;

        public CWinReg (IApplication app, IConfig config) {
            _app = app;
            _config = config;
        }

        public void SetWindowsStartup(bool startAtBoot) {
            string registryKeyPath = "SOFTWARE\\Microsoft\\Windows\\CurrentVersion\\Run";
            RegistryHive rh = RegistryHive.CurrentUser;
            RegistryView rv = Environment.Is64BitProcess ? RegistryView.Registry64 : RegistryView.Registry32;
            RegistryKeyPermissionCheck rkpc = RegistryKeyPermissionCheck.ReadWriteSubTree;
            RegistryRights rr = RegistryRights.FullControl;
            RegistryValueKind rvk = RegistryValueKind.String;
            using (RegistryKey bk = RegistryKey.OpenBaseKey(rh, rv)) {
                using (RegistryKey sk = bk.OpenSubKey(registryKeyPath, rkpc, rr)) {
                    if (sk != null) {
                        if (sk.GetValue(_app.AppName) != null) {
                            sk.DeleteValue(_app.AppName, false);
                        }
                        if (startAtBoot) {
                            sk.SetValue(_app.AppName, _app.AppPath, rvk);
                        }
                    }
                }
            }
        }

        public void ReSetConfigWindowsStartup() {
            SetWindowsStartup(_config.Get<bool>("WindowsStartup", bool.Parse(_app.GetConfig("windows_startup"))));
        }

    }

}
