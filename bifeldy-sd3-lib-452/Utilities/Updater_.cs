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
            this._app = app;
            this._logger = logger;
            this._config = config;
            this._converter = converter;
            this._db = db;

            this.UpdaterFtpIpDomain = this._config.Get<string>("UpdaterFtpIpDomain", this._app.GetConfig("updater_ftp_ip_domain"));
            this.UpdaterFtpPort = this._config.Get<int>("UpdaterFtpPort", int.Parse(this._app.GetConfig("updater_ftp_port")));
            this.UpdaterFtpUsername = this._config.Get<string>("UpdaterFtpUsername", this._app.GetConfig("updater_ftp_username"), true);
            this.UpdaterFtpPassword = this._config.Get<string>("UpdaterFtpPassword", this._app.GetConfig("updater_ftp_password"), true);
            this.ConnectionString = $"ftp://{this.UpdaterFtpIpDomain}:{this.UpdaterFtpPort}/{this.UpdaterWorkDir}";
        }

        public bool CheckUpdater(int newVersionTargetRequested = 0) {
            try {
                string localUpdaterPath = Path.Combine(this._app.AppLocation, this.UpdaterExeName);
                var uriUpdaterAppPath = new Uri($"{this.ConnectionString}/{this.UpdaterExeName}");
                var uriUpdaterAppVer = new Uri($"{this.ConnectionString}/{this.UpdaterVersion}");

                var webClient = new WebClient() {
                    Credentials = new NetworkCredential(this.UpdaterFtpUsername, this.UpdaterFtpPassword)
                };

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
                var updater = Process.Start(localUpdaterPath, $"--name \"{this._app.AppName}\" --version {newVersionTargetRequested} --pid {currentPid}");
                updater.WaitForExit();
            }
            catch (Exception ex) {
                this._logger.WriteError(ex);
            }

            return false;
        }

        public async Task UpdateSqliteDatabase() {
            var Directories = new List<string>();

            var ftpRequest = (FtpWebRequest) WebRequest.Create(new Uri(this.ConnectionString));
            ftpRequest.Credentials = new NetworkCredential(this.UpdaterFtpUsername, this.UpdaterFtpPassword);
            ftpRequest.Method = WebRequestMethods.Ftp.ListDirectory;

            var response = (FtpWebResponse) ftpRequest.GetResponse();
            using (var streamReader = new StreamReader(response.GetResponseStream())) {
                string line = streamReader.ReadLine();
                while (!string.IsNullOrEmpty(line)) {
                    Directories.Add(line);
                    line = streamReader.ReadLine();
                }
            }

            string jsonDbPath = Directories.Find(d => d.ToUpper().Contains($"{this._app.AppName}.json".ToUpper()));
            if (string.IsNullOrEmpty(jsonDbPath)) {
                return;
            }

            string jsonDb = null;
            using (var ftpClient = new WebClient()) {
                ftpClient.Credentials = new NetworkCredential(this.UpdaterFtpUsername, this.UpdaterFtpPassword);

                string ftpPathFile = this.ConnectionString.Replace(this.UpdaterWorkDir, "") + jsonDbPath;
                jsonDb = ftpClient.DownloadString(new Uri(ftpPathFile));
            }

            IDictionary<string, dynamic> dict = this._converter.JsonToObject<IDictionary<string, dynamic>>(jsonDb);
            foreach (KeyValuePair<string, dynamic> kvp in dict) {
                IDictionary<string, dynamic>[] tblRows = kvp.Value.ToObject<IDictionary<string, dynamic>[]>();
                foreach (IDictionary<string, dynamic> tblRow in tblRows) {
                    string sqlInsertColumnQuery = $" INSERT INTO {kvp.Key} ( ";
                    string sqlInsertValuesQuery = $" ) VALUES ( ";
                    var sqlInsertParam = new List<CDbQueryParamBind>();
                    string sqlUpdateQuery = $" UPDATE {kvp.Key} SET ";
                    var sqlUpdateParam = new List<CDbQueryParamBind>();
                    string sqlUpdateCondition = $" WHERE ";
                    long i = 0;
                    foreach (KeyValuePair<string, dynamic> row in tblRow) {
                        string column = row.Key;
                        dynamic value = row.Value;
                        if (i == 0) {
                            sqlUpdateCondition += $" {column} = :{column} ";
                        }
                        else {
                            if (i > 1) {
                                sqlUpdateQuery += " , ";
                            }

                            sqlUpdateQuery += $" {column} = :{column} ";
                        }

                        sqlUpdateParam.Add(new CDbQueryParamBind { NAME = column, VALUE = value });
                        if (i > 0) {
                            sqlInsertColumnQuery += " , ";
                            sqlInsertValuesQuery += " , ";
                        }

                        sqlInsertColumnQuery += $" {column} ";
                        sqlInsertValuesQuery += $" :{column} ";
                        sqlInsertParam.Add(new CDbQueryParamBind { NAME = column, VALUE = value });
                        i++;
                    }

                    try {
                        string sql = sqlInsertColumnQuery + sqlInsertValuesQuery + " ) ";
                        _ = await this._db.SQLite_ExecQuery(sql, sqlInsertParam);
                    }
                    catch {
                        string sql = sqlUpdateQuery + sqlUpdateCondition;
                        _ = await this._db.SQLite_ExecQuery(sql, sqlUpdateParam);
                    }
                }
            }
        }

    }

}
