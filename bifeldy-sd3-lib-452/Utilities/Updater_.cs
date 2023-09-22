/**
 * 
 * Author       :: Basilius Bias Astho Christyono
 * Phone        :: (+62) 889 236 6466
 * 
 * Department   :: IT SD 03
 * Mail         :: bias@indomaret.co.id
 * 
 * Catatan      :: Kirim Email
 *              :: Harap Didaftarkan Ke DI Container
 * 
 */

using System;
using System.Diagnostics;
using System.IO;
using System.Net;

namespace bifeldy_sd3_lib_452.Utilities {

    public interface IUpdater {
        bool CheckUpdater();
    }

    public sealed class CUpdater : IUpdater {

        private readonly IApplication _app;
        private readonly ILogger _logger;
        private readonly IConfig _config;

        private readonly string UpdaterWorkDir = "_updater";
        private readonly string UpdaterExeName = "updater.exe";
        private readonly string UpdaterVersion = "version.txt";

        public CUpdater(IApplication app, ILogger logger, IConfig config) {
            _app = app;
            _logger = logger;
            _config = config;
        }

        public bool CheckUpdater() {
            bool result = false;
            string updaterFtpIpDomain = _config.Get<string>("UpdaterFtpIpDomain", _app.GetConfig("updater_ftp_ip_domain"));
            string updaterFtpPort = _config.Get<string>("UpdaterFtpPort", _app.GetConfig("updater_ftp_port"));
            string updaterFtpUsername = _config.Get<string>("UpdaterFtpUsername", _app.GetConfig("updater_ftp_username"));
            string updaterFtpPassword = _config.Get<string>("UpdaterFtpPassword", _app.GetConfig("updater_ftp_password"));
            try {
                string connectionString = $"ftp://{updaterFtpUsername}:{updaterFtpPassword}@{updaterFtpIpDomain}:{updaterFtpPort}/{UpdaterWorkDir}";
                string localUpdaterPath = Path.Combine(_app.AppLocation, UpdaterExeName);
                Uri uriUpdaterAppPath = new Uri($"{connectionString}/{UpdaterExeName}");
                Uri uriUpdaterAppVer = new Uri($"{connectionString}/{UpdaterVersion}");
                WebClient webClient = new WebClient();
                if (!File.Exists(localUpdaterPath)) {
                    webClient.DownloadFile(uriUpdaterAppPath, localUpdaterPath);
                }
                int retry = 0;
                do {
                    retry++;
                    string updaterLocalVersion = string.Join("", FileVersionInfo.GetVersionInfo(localUpdaterPath).FileVersion.Split('.'));
                    string updaterRemoteVersion = webClient.DownloadString(uriUpdaterAppVer);
                    if (updaterRemoteVersion == updaterLocalVersion) {
                        break;
                    }
                    webClient.DownloadFile(uriUpdaterAppPath, localUpdaterPath);
                }
                while (retry < 3);
                int currentPid = Process.GetCurrentProcess().Id;
                Process updater = Process.Start(localUpdaterPath, $"\"{_app.AppName}\" {currentPid}");
                updater.WaitForExit();
            }
            catch (Exception ex) {
                _logger.WriteError(ex);
            }
            return result;
        }

    }

}
