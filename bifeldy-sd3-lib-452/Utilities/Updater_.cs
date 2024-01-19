/**
 * 
 * Author       :: Basilius Bias Astho Christyono
 * Phone        :: (+62) 889 236 6466
 * 
 * Department   :: IT SD 03
 * Mail         :: bias@indomaret.co.id
 * 
 * Catatan      :: Updater
 *              :: Harap Didaftarkan Ke DI Container
 * 
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Threading.Tasks;

using bifeldy_sd3_lib_452.Handlers;
using bifeldy_sd3_lib_452.Models;

namespace bifeldy_sd3_lib_452.Utilities {

    public interface IUpdater {
        bool CheckUpdater(int newVersionTargetRequested = 0);
        Task UpdateSqliteDatabase();
    }

    public sealed class CUpdater : IUpdater {

        private readonly IApplication _app;
        private readonly ILogger _logger;
        private readonly IConfig _config;
        private readonly IConverter _converter;
        private readonly IDbHandler _db;

        private readonly string UpdaterWorkDir = "_updater";
        private readonly string UpdaterExeName = "updater.exe";
        private readonly string UpdaterVersion = "version.txt";

        private string UpdaterFtpIpDomain { get; }
        private int UpdaterFtpPort { get; }
        private string UpdaterFtpUsername { get; }
        private string UpdaterFtpPassword { get; }

        private string ConnectionString { get; }

        public CUpdater(IApplication app, ILogger logger, IConfig config, IConverter converter, IDbHandler db) {
            _app = app;
            _logger = logger;
            _config = config;
            _converter = converter;
            _db = db;

            UpdaterFtpIpDomain = _config.Get<string>("UpdaterFtpIpDomain", _app.GetConfig("updater_ftp_ip_domain"));
            UpdaterFtpPort = _config.Get<int>("UpdaterFtpPort", int.Parse(_app.GetConfig("updater_ftp_port")));
            UpdaterFtpUsername = _config.Get<string>("UpdaterFtpUsername", _app.GetConfig("updater_ftp_username"), true);
            UpdaterFtpPassword = _config.Get<string>("UpdaterFtpPassword", _app.GetConfig("updater_ftp_password"), true);
            ConnectionString = $"ftp://{UpdaterFtpUsername}:{UpdaterFtpPassword}@{UpdaterFtpIpDomain}:{UpdaterFtpPort}/{UpdaterWorkDir}";
        }

        public bool CheckUpdater(int newVersionTargetRequested = 0) {
            try {
                string localUpdaterPath = Path.Combine(_app.AppLocation, UpdaterExeName);
                Uri uriUpdaterAppPath = new Uri($"{ConnectionString}/{UpdaterExeName}");
                Uri uriUpdaterAppVer = new Uri($"{ConnectionString}/{UpdaterVersion}");
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
                Process updater = Process.Start(localUpdaterPath, $"--name \"{_app.AppName}\" --version {newVersionTargetRequested} --pid {currentPid}");
                updater.WaitForExit();
            }
            catch (Exception ex) {
                _logger.WriteError(ex);
            }
            return false;
        }

        public async Task UpdateSqliteDatabase() {
            List<string> Directories = new List<string>();
            FtpWebRequest ftpRequest = (FtpWebRequest)WebRequest.Create(new Uri(ConnectionString));
            ftpRequest.Method = WebRequestMethods.Ftp.ListDirectory;
            FtpWebResponse response = (FtpWebResponse)ftpRequest.GetResponse();
            using (StreamReader streamReader = new StreamReader(response.GetResponseStream())) {
                string line = streamReader.ReadLine();
                while (!string.IsNullOrEmpty(line)) {
                    Directories.Add(line);
                    line = streamReader.ReadLine();
                }
            }
            string jsonDbPath = Directories.Find(d => d.ToUpper().Contains($"{_app.AppName}.json".ToUpper()));
            if (string.IsNullOrEmpty(jsonDbPath)) {
                return;
            }
            string ftpPathFile = ConnectionString.Replace(UpdaterWorkDir, "") + jsonDbPath;
            string jsonDb = new WebClient().DownloadString(new Uri(ftpPathFile));
            IDictionary<string, dynamic> dict = _converter.JsonToObject<IDictionary<string, dynamic>>(jsonDb);
            foreach (KeyValuePair<string, dynamic> kvp in dict) {
                IDictionary<string, dynamic>[] tblRows = kvp.Value.ToObject<IDictionary<string, dynamic>[]>();
                foreach (IDictionary<string, dynamic> tblRow in tblRows) {
                    string sqlInsertColumnQuery = $" INSERT INTO {kvp.Key} ( ";
                    string sqlInsertValuesQuery = $" ) VALUES ( ";
                    List<CDbQueryParamBind> sqlInsertParam = new List<CDbQueryParamBind>();
                    string sqlUpdateQuery = $" UPDATE {kvp.Key} SET ";
                    List<CDbQueryParamBind> sqlUpdateParam = new List<CDbQueryParamBind>();
                    string sqlUpdateCondition = $" WHERE ";
                    long i = 0;
                    foreach (KeyValuePair<string, dynamic> row in tblRow) {
                        string column = row.Key;
                        dynamic value = row.Value;
                        if (i == 0) {
                            sqlUpdateCondition += $" {column} = :{column} ";
                        }
                        else {
                            sqlUpdateQuery += $" {column} = :{column} ";
                        }
                        sqlUpdateParam.Add(new CDbQueryParamBind { NAME = column, VALUE = value });
                        if (i > 0) {
                            sqlInsertColumnQuery += " , ";
                            sqlInsertValuesQuery += " , ";
                        }
                        else if (i > 1) {
                            sqlUpdateQuery += " , ";
                        }
                        sqlInsertColumnQuery += $" {column} ";
                        sqlInsertValuesQuery += $" :{column} ";
                        sqlInsertParam.Add(new CDbQueryParamBind { NAME = column, VALUE = value });
                        i++;
                    }
                    try {
                        string sql = sqlInsertColumnQuery + sqlInsertValuesQuery + " ) ";
                        await _db.SQLite_ExecQuery(sql, sqlInsertParam);
                    }
                    catch {
                        string sql = sqlUpdateQuery + sqlUpdateCondition;
                        await _db.SQLite_ExecQuery(sql, sqlUpdateParam);
                    }
                }
            }
        }

    }

}
