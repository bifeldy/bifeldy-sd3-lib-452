/**
 * 
 * Author       :: Basilius Bias Astho Christyono
 * Phone        :: (+62) 889 236 6466
 * 
 * Department   :: IT SD 03
 * Mail         :: bias@indomaret.co.id
 * 
 * Catatan      :: Kumpulan Handler Database Bawaan
 *              :: Harap Didaftarkan Ke DI Container
 * 
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

using bifeldy_sd3_lib_452.Abstractions;
using bifeldy_sd3_lib_452.Databases;
using bifeldy_sd3_lib_452.Models;
using bifeldy_sd3_lib_452.Utilities;

namespace bifeldy_sd3_lib_452.Handlers {

    public interface IDbHandler {
        bool LocalDbOnly { get; }
        string LoggedInUsername { get; set; }
        string DbName { get; }
        string GetAllAvailableDbConnectionsString();
        void CloseAllConnection(bool force = false);
        Task MarkBeforeCommitRollback();
        void MarkSuccessCommitAndClose();
        bool MarkFailedRollbackAndClose();
        IOracle NewExternalConnectionOra(string dbIpAddrss, string dbPort, string dbUsername, string dbPassword, string dbNameSid);
        IPostgres NewExternalConnectionPg(string dbIpAddrss, string dbPort, string dbUsername, string dbPassword, string dbName);
        IMsSQL NewExternalConnectionMsSql(string dbIpAddrss, string dbUsername, string dbPassword, string dbName);
        IMySQL NewExternalConnectionMySql(string dbIpAddrss, string dbUsername, string dbPassword, string dbName);
        ISqlite NewExternalConnectionSqlite(string dbName);
        Task<string> GetJenisDc();
        Task<string> GetKodeDc();
        Task<string> GetNamaDc();
        Task<bool> IsDcHo();
        Task<bool> IsWhHo();
        Task<bool> IsHo();
        Task<string> CekVersi();
        Task<bool> LoginUser(string usernameNik, string password);
        Task<bool> CheckIpMac();
        Task<string> OraPg_GetURLWebService(string webType);
        Task<T> OraPg_GetMailInfo<T>(string kolom);
        Task<DateTime> OraPg_GetYesterdayDate(int lastDay);
        Task<DateTime> OraPg_GetLastMonth(int lastMonth);
        Task<DateTime> OraPg_GetCurrentTimestamp();
        Task<DateTime> OraPg_GetCurrentDate();
        Task<bool> OraPg_SaveKafkaToTable(string topic, decimal offset, decimal partition, KafkaMessage<string, string> msg, string tabelName = "DC_KAFKALOG_T");
        Task<bool> OraPg_AlterTable_AddColumnIfNotExist(string tableName, string columnName, string columnType);
        Task<bool> OraPg_TruncateTable(string TableName);
        Task<bool> OraPg_BulkInsertInto(string tableName, DataTable dataTable);
        Task<List<string>> OraPg_RetrieveBlob(string stringPathDownload, string queryString, List<CDbQueryParamBind> bindParam = null, string stringCustomSingleFileName = null);
        Task<string> OraPg_BulkGetCsv(string rawQueryVulnerableSqlInjection, string delimiter, string filename, string outputPath = null, bool rawQueryWithoutParam = false, bool doubleQuote = true);
        Task<T> OraPg_ExecScalar<T>(string sqlQuery, List<CDbQueryParamBind> bindParam = null);
        Task<bool> OraPg_ExecQuery(string sqlQuery, List<CDbQueryParamBind> bindParam = null);
        Task<DbDataReader> OraPg_ExecReaderAsync(string sqlQuery, List<CDbQueryParamBind> bindParam = null);
        Task<DataTable> OraPg_GetDataTable(string sqlQuery, List<CDbQueryParamBind> bindParam = null);
        Task<List<T>> OraPg_GetList<T>(string sqlQuery, List<CDbQueryParamBind> bindParam = null);
        Task<CDbExecProcResult> OraPg_CALL_(string procName, List<CDbQueryParamBind> bindParam = null);
        Task<bool> Oracle_TruncateTable(string TableName);
        Task<bool> Oracle_BulkInsertInto(string tableName, DataTable dataTable);
        Task<T> Oracle_ExecScalar<T>(string sqlQuery, List<CDbQueryParamBind> bindParam = null);
        Task<bool> Oracle_ExecQuery(string sqlQuery, List<CDbQueryParamBind> bindParam = null);
        Task<DbDataReader> Oracle_ExecReaderAsync(string sqlQuery, List<CDbQueryParamBind> bindParam = null);
        Task<DataTable> Oracle_GetDataTable(string sqlQuery, List<CDbQueryParamBind> bindParam = null);
        Task<List<T>> Oracle_GetList<T>(string sqlQuery, List<CDbQueryParamBind> bindParam = null);
        Task<CDbExecProcResult> Oracle_CALL_(string procedureName, List<CDbQueryParamBind> bindParam = null);
        Task<List<string>> Oracle_RetrieveBlob(string stringPathDownload, string queryString, List<CDbQueryParamBind> bindParam = null, string stringCustomSingleFileName = null);
        Task<string> Oracle_BulkGetCsv(string rawQueryVulnerableSqlInjection, string delimiter, string filename, string outputPath = null, bool rawQueryWithoutParam = false, bool doubleQuote = true);
        Task<bool> Postgres_TruncateTable(string TableName);
        Task<bool> Postgres_BulkInsertInto(string tableName, DataTable dataTable);
        Task<T> Postgres_ExecScalar<T>(string sqlQuery, List<CDbQueryParamBind> bindParam = null);
        Task<bool> Postgres_ExecQuery(string sqlQuery, List<CDbQueryParamBind> bindParam = null);
        Task<DbDataReader> Postgres_ExecReaderAsync(string sqlQuery, List<CDbQueryParamBind> bindParam = null);
        Task<DataTable> Postgres_GetDataTable(string sqlQuery, List<CDbQueryParamBind> bindParam = null);
        Task<List<T>> Postgres_GetList<T>(string sqlQuery, List<CDbQueryParamBind> bindParam = null);
        Task<CDbExecProcResult> Postgres_CALL_(string procedureName, List<CDbQueryParamBind> bindParam = null);
        Task<List<string>> Postgres_RetrieveBlob(string stringPathDownload, string queryString, List<CDbQueryParamBind> bindParam = null, string stringCustomSingleFileName = null);
        Task<string> Postgres_BulkGetCsv(string rawQueryVulnerableSqlInjection, string delimiter, string filename, string outputPath = null, bool rawQueryWithoutParam = false, bool doubleQuote = true);
        Task<bool> MsSql_TruncateTable(string TableName);
        Task<bool> MsSql_BulkInsertInto(string tableName, DataTable dataTable);
        Task<T> MsSql_ExecScalar<T>(string sqlQuery, List<CDbQueryParamBind> bindParam = null);
        Task<bool> MsSql_ExecQuery(string sqlQuery, List<CDbQueryParamBind> bindParam = null);
        Task<DbDataReader> MsSql_ExecReaderAsync(string sqlQuery, List<CDbQueryParamBind> bindParam = null);
        Task<DataTable> MsSql_GetDataTable(string sqlQuery, List<CDbQueryParamBind> bindParam = null);
        Task<List<T>> MsSql_GetList<T>(string sqlQuery, List<CDbQueryParamBind> bindParam = null);
        Task<CDbExecProcResult> MsSql_CALL_(string procedureName, List<CDbQueryParamBind> bindParam = null);
        Task<List<string>> MsSql_RetrieveBlob(string stringPathDownload, string queryString, List<CDbQueryParamBind> bindParam = null, string stringCustomSingleFileName = null);
        Task<string> MsSql_BulkGetCsv(string rawQueryVulnerableSqlInjection, string delimiter, string filename, string outputPath = null, bool rawQueryWithoutParam = false, bool doubleQuote = true);
        Task<bool> MySql_TruncateTable(string TableName);
        Task<bool> MySql_BulkInsertInto(string tableName, DataTable dataTable);
        Task<T> MySql_ExecScalar<T>(string sqlQuery, List<CDbQueryParamBind> bindParam = null);
        Task<bool> MySql_ExecQuery(string sqlQuery, List<CDbQueryParamBind> bindParam = null);
        Task<DbDataReader> MySql_ExecReaderAsync(string sqlQuery, List<CDbQueryParamBind> bindParam = null);
        Task<DataTable> MySql_GetDataTable(string sqlQuery, List<CDbQueryParamBind> bindParam = null);
        Task<List<T>> MySql_GetList<T>(string sqlQuery, List<CDbQueryParamBind> bindParam = null);
        Task<CDbExecProcResult> MySql_CALL_(string procedureName, List<CDbQueryParamBind> bindParam = null);
        Task<List<string>> MySql_RetrieveBlob(string stringPathDownload, string queryString, List<CDbQueryParamBind> bindParam = null, string stringCustomSingleFileName = null);
        Task<string> MySql_BulkGetCsv(string rawQueryVulnerableSqlInjection, string delimiter, string filename, string outputPath = null, bool rawQueryWithoutParam = false, bool doubleQuote = true);
        Task<bool> SQLite_TruncateTable(string TableName);
        Task<bool> SQLite_BulkInsertInto(string tableName, DataTable dataTable);
        Task<T> SQLite_ExecScalar<T>(string sqlQuery, List<CDbQueryParamBind> bindParam = null);
        Task<bool> SQLite_ExecQuery(string sqlQuery, List<CDbQueryParamBind> bindParam = null);
        Task<DbDataReader> SQLite_ExecReaderAsync(string sqlQuery, List<CDbQueryParamBind> bindParam = null);
        Task<DataTable> SQLite_GetDataTable(string sqlQuery, List<CDbQueryParamBind> bindParam = null);
        Task<List<T>> SQLite_GetList<T>(string sqlQuery, List<CDbQueryParamBind> bindParam = null);
        Task<List<string>> SQLite_RetrieveBlob(string stringPathDownload, string queryString, List<CDbQueryParamBind> bindParam = null, string stringCustomSingleFileName = null);
        Task<string> SQLite_BulkGetCsv(string rawQueryVulnerableSqlInjection, string delimiter, string filename, string outputPath = null, bool rawQueryWithoutParam = false, bool doubleQuote = true);
    }

    public class CDbHandler : IDbHandler {

        public bool LocalDbOnly { get; }

        private readonly IApplication _app;

        private readonly IOracle _oracle;
        private readonly IPostgres _postgres;
        private readonly IMsSQL _mssql;
        private readonly IMySQL _mysql;
        private readonly ISqlite _sqlite;

        private string DcCode = null;
        private string DcName = null;
        private string DcJenis = null;

        public string LoggedInUsername { get; set; }

        public CDbHandler(IApplication app, IConfig config, IOracle oracle, IPostgres postgres, IMsSQL mssql, IMySQL mysql, ISqlite sqlite) {
            this.LocalDbOnly = config.Get<bool>("LocalDbOnly", bool.Parse(app.GetConfig("local_db_only")));

            this._app = app;
            this._oracle = oracle;
            this._postgres = postgres;
            this._mssql = mssql;
            this._mysql = mysql;
            this._sqlite = sqlite;
        }

        protected IOracle Oracle {
            get {
                if (this.LocalDbOnly) {
                    return null;
                }

                IOracle ret = (bool)this._oracle?.Available ? this._oracle : null;
                return ret ?? throw new Exception("Gagal Membaca Dan Mengambil `Kunci` Oracle Database");
            }
        }

        protected IPostgres Postgres {
            get {
                if (this.LocalDbOnly) {
                    return null;
                }

                IPostgres ret = (bool)this._postgres?.Available ? this._postgres : null;
                return ret ?? throw new Exception("Gagal Membaca Dan Mengambil `Kunci` Postgres Database");
            }
        }

        protected IDatabase OraPg => this.LocalDbOnly ? null : this._app.IsUsingPostgres ? this.Postgres : (IDatabase)this.Oracle;

        protected IMsSQL MsSql {
            get {
                IMsSQL ret = (bool)this._mssql?.Available ? this._mssql : null;
                return ret ?? throw new Exception("Gagal Membaca Dan Mengambil `Kunci` Ms. SQL Server Database");
            }
        }

        protected IMySQL MySql {
            get {
                IMySQL ret = (bool)this._mysql?.Available ? this._mysql : null;
                return ret ?? throw new Exception("Gagal Membaca Dan Mengambil `Kunci` MySQL Server Database");
            }
        }

        protected ISqlite Sqlite {
            get {
                ISqlite ret = (bool)this._sqlite?.Available ? this._sqlite : null;
                return ret ?? throw new Exception("Gagal Membaca Dan Mengambil `Kunci` SQLite Database");
            }
        }

        /** Custom Queries */

        public string DbName {
            get {
                if (this.LocalDbOnly) {
                    return this.Sqlite.DbName?.Replace("\\", "/").Split('/').Last();
                }

                string FullDbName = string.Empty;
                try {
                    FullDbName += this.Oracle.DbName;
                }
                catch {
                    FullDbName += "-";
                }

                FullDbName += " / ";
                try {
                    FullDbName += this.Postgres.DbName;
                }
                catch {
                    FullDbName += "-";
                }

                FullDbName += " / ";
                try {
                    FullDbName += this.MsSql.DbName;
                }
                catch {
                    FullDbName += "-";
                }

                FullDbName += " / ";
                try {
                    FullDbName += this.MySql.DbName;
                }
                catch {
                    FullDbName += "-";
                }

                FullDbName += " / ";
                try {
                    FullDbName += this.Sqlite.DbName;
                }
                catch {
                    FullDbName += "-";
                }

                return FullDbName;
            }
        }

        public string GetAllAvailableDbConnectionsString() {
            string newLine = Environment.NewLine;
            // Bypass Check DB Availablility ~
            string oracle = $"Oracle :: {this._oracle?.DbName}{newLine}{newLine}{this._oracle?.DbConnectionString}{newLine}{newLine}{newLine}";
            string postgre = $"Postgres :: {this._postgres?.DbName}{newLine}{newLine}{this._postgres?.DbConnectionString}{newLine}{newLine}{newLine}";
            string mssql = $"MsSql :: {this._mssql?.DbName}{newLine}{newLine}{this._mssql?.DbConnectionString}{newLine}{newLine}{newLine}";
            string mysql = $"MySql :: {this._mysql?.DbName}{newLine}{newLine}{this._mysql?.DbConnectionString}{newLine}{newLine}{newLine}";
            string sqlite = $"SQLite :: {this._sqlite?.DbName?.Replace("\\", "/").Split('/').Last()}{newLine}{newLine}{this._sqlite?.DbConnectionString}";
            return oracle + postgre + mssql + mysql + sqlite;
        }

        public void CloseAllConnection(bool force = false) {
            this._oracle?.CloseConnection(force);
            this._postgres?.CloseConnection(force);
            this._mssql?.CloseConnection(force);
            this._mysql?.CloseConnection(force);
            this._sqlite?.CloseConnection(force);
        }

        public async Task MarkBeforeCommitRollback() {
            await this._oracle?.MarkBeforeCommitRollback();
            await this._postgres?.MarkBeforeCommitRollback();
            await this._mssql?.MarkBeforeCommitRollback();
            await this._mysql?.MarkBeforeCommitRollback();
            await this._sqlite?.MarkBeforeCommitRollback();
        }

        public void MarkSuccessCommitAndClose() {
            this._oracle?.MarkSuccessCommitAndClose();
            this._postgres?.MarkSuccessCommitAndClose();
            this._mssql?.MarkSuccessCommitAndClose();
            this._mysql?.MarkSuccessCommitAndClose();
            this._sqlite?.MarkSuccessCommitAndClose();
        }

        public bool MarkFailedRollbackAndClose() {
            try {
                this._oracle?.MarkFailedRollbackAndClose();
                this._postgres?.MarkFailedRollbackAndClose();
                this._mssql?.MarkFailedRollbackAndClose();
                this._mysql?.MarkFailedRollbackAndClose();
                this._sqlite?.MarkFailedRollbackAndClose();
                return true;
            }
            catch {
                if (Environment.UserInteractive) {
                    _ = MessageBox.Show(
                        "Data Tidak Tersimpan, Silahkan Ulangi Kembali ~",
                        "Gagal ROLLBACK",
                        MessageBoxButtons.OK,
                        MessageBoxIcon.Error
                    );
                }

                return false;
            }
        }

        /* Perlakuan Khusus */

        public IOracle NewExternalConnectionOra(string dbIpAddrss, string dbPort, string dbUsername, string dbPassword, string dbNameSid) {
            return this._oracle?.NewExternalConnection(dbIpAddrss, dbPort, dbUsername, dbPassword, dbNameSid);
        }

        public IPostgres NewExternalConnectionPg(string dbIpAddrss, string dbPort, string dbUsername, string dbPassword, string dbName) {
            return this._postgres?.NewExternalConnection(dbIpAddrss, dbPort, dbUsername, dbPassword, dbName);
        }

        public IMsSQL NewExternalConnectionMsSql(string dbIpAddrss, string dbUsername, string dbPassword, string dbName) {
            return this._mssql?.NewExternalConnection(dbIpAddrss, dbUsername, dbPassword, dbName);
        }

        public IMySQL NewExternalConnectionMySql(string dbIpAddrss, string dbUsername, string dbPassword, string dbName) {
            return this._mysql?.NewExternalConnection(dbIpAddrss, dbUsername, dbPassword, dbName);
        }

        public ISqlite NewExternalConnectionSqlite(string dbName) {
            return this._sqlite?.NewExternalConnection(dbName);
        }

        /* ** */

        public async Task<string> GetJenisDc() {
            if (this.LocalDbOnly) {
                return "NONDC";
            }

            if ((bool)this.OraPg?.DbUsername.ToUpper().Contains("DCHO") || (bool)this.OraPg?.DbUsername.ToUpper().Contains("WHHO")) {
                return "HO";
            }

            if (string.IsNullOrEmpty(this.DcJenis)) {
                this.DcJenis = await this.OraPg?.ExecScalarAsync<string>("SELECT TBL_JENIS_DC FROM DC_TABEL_DC_T");
            }

            return this.DcJenis.ToUpper();
        }

        public async Task<string> GetKodeDc() {
            if (this.LocalDbOnly) {
                return "GXXX";
            }

            if ((bool)this.OraPg?.DbUsername.ToUpper().Contains("DCHO")) {
                return "DCHO";
            }
            else if ((bool)this.OraPg?.DbUsername.ToUpper().Contains("WHHO")) {
                return "WHHO";
            }

            if (string.IsNullOrEmpty(this.DcCode)) {
                this.DcCode = await this.OraPg?.ExecScalarAsync<string>("SELECT TBL_DC_KODE FROM DC_TABEL_DC_T");
            }

            return this.DcCode.ToUpper();
        }

        public async Task<string> GetNamaDc() {
            if (this.LocalDbOnly) {
                return "NON DC";
            }

            if ((bool)this.OraPg?.DbUsername.ToUpper().Contains("DCHO")) {
                return "DC HEAD OFFICE";
            }
            else if ((bool)this.OraPg?.DbUsername.ToUpper().Contains("WHHO")) {
                return "WH HEAD OFFICE";
            }

            if (string.IsNullOrEmpty(this.DcName)) {
                this.DcName = await this.OraPg?.ExecScalarAsync<string>("SELECT TBL_DC_NAMA FROM DC_TABEL_DC_T");
            }

            return this.DcName.ToUpper();
        }

        public async Task<bool> IsDcHo() {
            string kodeDc = await this.GetKodeDc();
            return kodeDc == "DCHO";
        }

        public async Task<bool> IsWhHo() {
            string kodeDc = await this.GetKodeDc();
            return kodeDc == "WHHO";
        }

        public async Task<bool> IsHo() {
            bool isDcHo = await this.IsDcHo();
            bool isWhHo = await this.IsWhHo();
            return isDcHo || isWhHo;
        }

        public async Task<string> CekVersi() {
            if (this._app.DebugMode || this.LocalDbOnly) {
                return "OKE";
            }
            else {
                try {
                    string res1 = await this.OraPg?.ExecScalarAsync<string>(
                        $@"
                            SELECT
                                CASE
                                    WHEN COALESCE(aprove, 'N') = 'Y' AND {(
                                            this._app.IsUsingPostgres
                                                ? "COALESCE(tgl_berlaku, NOW())::DATE <= CURRENT_DATE"
                                                : "TRUNC(COALESCE(tgl_berlaku, SYSDATE)) <= TRUNC(SYSDATE)"
                                        )} 
                                        THEN COALESCE(VERSI_BARU, '0')
                                    WHEN COALESCE(aprove, 'N') = 'N'
                                        THEN COALESCE(versi_lama, '0')
                                    ELSE
                                        COALESCE(versi_lama, '0')
                                END AS VERSI
                            FROM
                                dc_program_vbdtl_t
                            WHERE
                                UPPER(dc_kode) = :dc_kode
                                AND UPPER(nama_prog) LIKE :nama_prog
                        ",
                        new List<CDbQueryParamBind> {
                            new CDbQueryParamBind { NAME = "dc_kode", VALUE = await this.GetKodeDc() },
                            new CDbQueryParamBind { NAME = "nama_prog", VALUE = $"%{this._app.AppName}%" }
                        }
                    );
                    if (string.IsNullOrEmpty(res1)) {
                        return $"Program :: {this._app.AppName}" + Environment.NewLine + "Belum Terdaftar Di Master Program DC";
                    }

                    if (res1 == this._app.AppVersion) {
                        try {
                            bool res2 = await this.OraPg?.ExecQueryAsync(
                                $@"
                                    INSERT INTO dc_monitoring_program_t (kode_dc, nama_program, ip_client, versi, tanggal)
                                    VALUES (:kode_dc, :nama_program, :ip_client, :versi, {(this._app.IsUsingPostgres ? "NOW()" : "SYSDATE")})
                                ",
                                new List<CDbQueryParamBind> {
                                    new CDbQueryParamBind { NAME = "kode_dc", VALUE = await this.GetKodeDc() },
                                    new CDbQueryParamBind { NAME = "nama_program", VALUE = this._app.AppName },
                                    new CDbQueryParamBind { NAME = "ip_client", VALUE = this._app.GetAllIpAddress().FirstOrDefault() },
                                    new CDbQueryParamBind { NAME = "versi", VALUE = this._app.AppVersion }
                                }
                            );
                            return "OKE";
                        }
                        catch (Exception ex2) {
                            return ex2.Message;
                        }
                    }
                    else {
                        return $"Versi Program :: {this._app.AppName}" + Environment.NewLine + $"Tidak Sama Dengan Master Program = v{res1}";
                    }
                }
                catch (Exception ex1) {
                    return ex1.Message;
                }
            }
        }

        public async Task<bool> LoginUser(string userNameNik, string password) {
            string query = $@"
                SELECT
                    {(this.LocalDbOnly ? "uname" : "user_name")}
                FROM
                    {(this.LocalDbOnly ? "users" : "dc_user_t")}
                WHERE
                    {(this.LocalDbOnly ? @"
                        UPPER(uname) = UPPER(:uname)
                        AND UPPER(upswd) = UPPER(:pass)
                    " : @"
                        (UPPER(user_name) = UPPER(:uname) OR UPPER(user_nik) = UPPER(:unik))
                        AND UPPER(user_password) = UPPER(:pass)
                    ")}
            ";
            var param = new List<CDbQueryParamBind> {
                new CDbQueryParamBind { NAME = "uname", VALUE = userNameNik }
            };
            if (string.IsNullOrEmpty(this.LoggedInUsername)) {
                if (this.LocalDbOnly) {
                    byte[] pswd = new SHA1Managed().ComputeHash(Encoding.UTF8.GetBytes(password));
                    string hash = string.Concat(pswd.Select(b => b.ToString("x2")));
                    param.Add(new CDbQueryParamBind { NAME = "pass", VALUE = hash });
                    this.LoggedInUsername = await this.Sqlite.ExecScalarAsync<string>(query, param);
                }
                else {
                    param.Add(new CDbQueryParamBind { NAME = "unik", VALUE = userNameNik });
                    param.Add(new CDbQueryParamBind { NAME = "pass", VALUE = password });
                    this.LoggedInUsername = await this.OraPg?.ExecScalarAsync<string>(query, param);
                }
            }

            return !string.IsNullOrEmpty(this.LoggedInUsername);
        }

        public async Task<bool> CheckIpMac() {
            if (this._app.DebugMode || this.LocalDbOnly) {
                return true;
            }
            else {
                string res = await this.OraPg?.ExecScalarAsync<string>(
                    $@"
                        SELECT
                            a.user_name
                        FROM
                            dc_user_t a,
                            dc_ipaddress_t b,
                            dc_security_t c
                        WHERE
                            a.user_name = b.ip_fk_user_name AND
                            b.ip_fk_group_name = c.fk_user_name AND
                            UPPER(a.user_name) = UPPER(:user_name) AND
                            (UPPER(b.ip_addr) IN (:ip_v4_v6) OR UPPER(b.ip_addr) IN (:mac_addr)) AND
                            UPPER(c.sec_app_name) LIKE :app_name
                    ",
                    new List<CDbQueryParamBind> {
                        new CDbQueryParamBind { NAME = "user_name", VALUE = this.LoggedInUsername },
                        new CDbQueryParamBind { NAME = "ip_v4_v6", VALUE = this._app.GetAllIpAddress() },
                        new CDbQueryParamBind { NAME = "mac_addr", VALUE = this._app.GetAllMacAddress() },
                        new CDbQueryParamBind { NAME = "app_name", VALUE = $"%{this._app.AppName}%" }
                    }
                );
                return res == this.LoggedInUsername;
            }
        }

        public async Task<string> OraPg_GetURLWebService(string webType) {
            return await this.OraPg?.ExecScalarAsync<string>(
                $@"SELECT WEB_URL FROM DC_WEBSERVICE_T WHERE WEB_TYPE = :web_type",
                new List<CDbQueryParamBind> {
                    new CDbQueryParamBind { NAME = "web_type", VALUE = webType }
                }
            );
        }

        public async Task<T> OraPg_GetMailInfo<T>(string kolom) {
            return await this.OraPg?.ExecScalarAsync<T>(
                $@"SELECT {kolom} FROM DC_LISTMAILSERVER_T WHERE MAIL_DCKODE = :mail_dckode",
                new List<CDbQueryParamBind> {
                    new CDbQueryParamBind { NAME = "mail_dckode", VALUE = await this.GetKodeDc() }
                }
            );
        }

        public async Task<DateTime> OraPg_GetYesterdayDate(int lastDay) {
            return await this.OraPg?.ExecScalarAsync<DateTime>(
                $@"
                    SELECT {(this._app.IsUsingPostgres ? "CURRENT_DATE" : "TRUNC(SYSDATE)")} - :last_day
                    {(this._app.IsUsingPostgres ? "" : "FROM DUAL")}
                ",
                new List<CDbQueryParamBind> {
                    new CDbQueryParamBind { NAME = "last_day", VALUE = lastDay }
                }
            );
        }

        public async Task<DateTime> OraPg_GetLastMonth(int lastMonth) {
            return await this.OraPg?.ExecScalarAsync<DateTime>(
                $@"
                    SELECT TRUNC(add_months({(this._app.IsUsingPostgres ? "CURRENT_DATE" : "SYSDATE")}, - :last_month))
                    {(this._app.IsUsingPostgres ? "" : "FROM DUAL")}
                ",
                new List<CDbQueryParamBind> {
                    new CDbQueryParamBind { NAME = "last_month", VALUE = lastMonth }
                }
            );
        }

        public async Task<DateTime> OraPg_GetCurrentTimestamp() {
            return await this.OraPg?.ExecScalarAsync<DateTime>($@"
                SELECT {(this._app.IsUsingPostgres ? "CURRENT_TIMESTAMP" : "SYSDATE FROM DUAL")}
            ");
        }

        public async Task<DateTime> OraPg_GetCurrentDate() {
            return await this.OraPg?.ExecScalarAsync<DateTime>($@"
                SELECT {(this._app.IsUsingPostgres ? "CURRENT_DATE" : "TRUNC(SYSDATE) FROM DUAL")}
            ");
        }

        public async Task<bool> OraPg_SaveKafkaToTable(string topic, decimal offset, decimal partition, KafkaMessage<string, string> msg, string tabelName = "DC_KAFKALOG_T") {
            return await this.OraPg?.ExecQueryAsync($@"
                INSERT INTO {tabelName} (TPC, OFFS, PARTT, KEY, VAL, TMSTAMP)
                VALUES (:tpc, :offs, :partt, :key, :value, :tmstmp)
            ", new List<CDbQueryParamBind> {
                new CDbQueryParamBind { NAME = "tpc", VALUE = topic },
                new CDbQueryParamBind { NAME = "offs", VALUE = offset },
                new CDbQueryParamBind { NAME = "partt", VALUE = partition },
                new CDbQueryParamBind { NAME = "key", VALUE = msg.Key },
                new CDbQueryParamBind { NAME = "value", VALUE = msg.Value },
                new CDbQueryParamBind { NAME = "tmstmp", VALUE = msg.Timestamp.UtcDateTime }
            });
        }

        // Bisa Kena SQL Injection
        public async Task<bool> OraPg_AlterTable_AddColumnIfNotExist(string tableName, string columnName, string columnType) {
            var cols = new List<string>();
            DataColumnCollection columns = await this.OraPg?.GetAllColumnTableAsync(tableName);
            foreach (DataColumn col in columns) {
                cols.Add(col.ColumnName.ToUpper());
            }

            return !cols.Contains(columnName.ToUpper()) && await this.OraPg?.ExecQueryAsync($@"
                ALTER TABLE {tableName}
                    ADD {(this._app.IsUsingPostgres ? "COLUMN" : "(")}
                        {columnName} {columnType}
                    {(this._app.IsUsingPostgres ? "" : ")")}
            ");
        }

        /* ** */

        public async Task<bool> OraPg_TruncateTable(string TableName) {
            return await this.OraPg?.ExecQueryAsync($@"TRUNCATE TABLE {TableName}");
        }

        public async Task<bool> OraPg_BulkInsertInto(string tableName, DataTable dataTable) {
            return await this.OraPg?.BulkInsertInto(tableName, dataTable);
        }

        public async Task<T> OraPg_ExecScalar<T>(string sqlQuery, List<CDbQueryParamBind> bindParam = null) {
            return await this.OraPg?.ExecScalarAsync<T>(sqlQuery, bindParam);
        }

        public async Task<bool> OraPg_ExecQuery(string sqlQuery, List<CDbQueryParamBind> bindParam = null) {
            return await this.OraPg?.ExecQueryAsync(sqlQuery, bindParam);
        }

        public async Task<DbDataReader> OraPg_ExecReaderAsync(string sqlQuery, List<CDbQueryParamBind> bindParam = null) {
            return await this.OraPg?.ExecReaderAsync(sqlQuery, bindParam);
        }

        public async Task<DataTable> OraPg_GetDataTable(string sqlQuery, List<CDbQueryParamBind> bindParam = null) {
            return await this.OraPg?.GetDataTableAsync(sqlQuery, bindParam);
        }

        public async Task<List<T>> OraPg_GetList<T>(string sqlQuery, List<CDbQueryParamBind> bindParam = null) {
            return await this.OraPg?.GetListAsync<T>(sqlQuery, bindParam);
        }

        public async Task<CDbExecProcResult> OraPg_CALL_(string procedureName, List<CDbQueryParamBind> bindParam = null) {
            return await this.OraPg?.ExecProcedureAsync(procedureName, bindParam);
        }

        public async Task<List<string>> OraPg_RetrieveBlob(string stringPathDownload, string queryString, List<CDbQueryParamBind> bindParam = null, string stringCustomSingleFileName = null) {
            return await this.OraPg?.RetrieveBlob(stringPathDownload, queryString, bindParam, stringCustomSingleFileName);
        }

        public async Task<string> OraPg_BulkGetCsv(string rawQueryVulnerableSqlInjection, string delimiter, string filename, string outputPath = null, bool rawQueryWithoutParam = false, bool doubleQuote = true) {
            return await this.OraPg?.BulkGetCsv(rawQueryVulnerableSqlInjection, delimiter, filename, outputFolderPath: outputPath, useRawQueryWithoutParam: rawQueryWithoutParam, useDoubleQuote: doubleQuote);
        }

        /* ** */

        public async Task<bool> Oracle_TruncateTable(string TableName) {
            return await this.Oracle?.ExecQueryAsync($@"TRUNCATE TABLE {TableName}");
        }

        public async Task<bool> Oracle_BulkInsertInto(string tableName, DataTable dataTable) {
            return await this.Oracle?.BulkInsertInto(tableName, dataTable);
        }

        public async Task<T> Oracle_ExecScalar<T>(string sqlQuery, List<CDbQueryParamBind> bindParam = null) {
            return await this.Oracle?.ExecScalarAsync<T>(sqlQuery, bindParam);
        }

        public async Task<bool> Oracle_ExecQuery(string sqlQuery, List<CDbQueryParamBind> bindParam = null) {
            return await this.Oracle?.ExecQueryAsync(sqlQuery, bindParam);
        }

        public async Task<DbDataReader> Oracle_ExecReaderAsync(string sqlQuery, List<CDbQueryParamBind> bindParam = null) {
            return await this.Oracle?.ExecReaderAsync(sqlQuery, bindParam);
        }

        public async Task<DataTable> Oracle_GetDataTable(string sqlQuery, List<CDbQueryParamBind> bindParam = null) {
            return await this.Oracle?.GetDataTableAsync(sqlQuery, bindParam);
        }

        public async Task<List<T>> Oracle_GetList<T>(string sqlQuery, List<CDbQueryParamBind> bindParam = null) {
            return await this.Oracle?.GetListAsync<T>(sqlQuery, bindParam);
        }

        public async Task<CDbExecProcResult> Oracle_CALL_(string procedureName, List<CDbQueryParamBind> bindParam = null) {
            return await this.Oracle?.ExecProcedureAsync(procedureName, bindParam);
        }

        public async Task<List<string>> Oracle_RetrieveBlob(string stringPathDownload, string queryString, List<CDbQueryParamBind> bindParam = null, string stringCustomSingleFileName = null) {
            return await this.Oracle?.RetrieveBlob(stringPathDownload, queryString, bindParam, stringCustomSingleFileName);
        }

        public async Task<string> Oracle_BulkGetCsv(string rawQueryVulnerableSqlInjection, string delimiter, string filename, string outputPath = null, bool rawQueryWithoutParam = false, bool doubleQuote = true) {
            return await this.Oracle?.BulkGetCsv(rawQueryVulnerableSqlInjection, delimiter, filename, outputFolderPath: outputPath, useRawQueryWithoutParam: rawQueryWithoutParam, useDoubleQuote: doubleQuote);
        }

        /* ** */

        public async Task<bool> Postgres_TruncateTable(string TableName) {
            return await this.Postgres?.ExecQueryAsync($@"TRUNCATE TABLE {TableName}");
        }

        public async Task<bool> Postgres_BulkInsertInto(string tableName, DataTable dataTable) {
            return await this.Postgres?.BulkInsertInto(tableName, dataTable);
        }

        public async Task<T> Postgres_ExecScalar<T>(string sqlQuery, List<CDbQueryParamBind> bindParam = null) {
            return await this.Postgres?.ExecScalarAsync<T>(sqlQuery, bindParam);
        }

        public async Task<bool> Postgres_ExecQuery(string sqlQuery, List<CDbQueryParamBind> bindParam = null) {
            return await this.Postgres?.ExecQueryAsync(sqlQuery, bindParam);
        }

        public async Task<DbDataReader> Postgres_ExecReaderAsync(string sqlQuery, List<CDbQueryParamBind> bindParam = null) {
            return await this.Postgres?.ExecReaderAsync(sqlQuery, bindParam);
        }

        public async Task<DataTable> Postgres_GetDataTable(string sqlQuery, List<CDbQueryParamBind> bindParam = null) {
            return await this.Postgres?.GetDataTableAsync(sqlQuery, bindParam);
        }

        public async Task<List<T>> Postgres_GetList<T>(string sqlQuery, List<CDbQueryParamBind> bindParam = null) {
            return await this.Postgres?.GetListAsync<T>(sqlQuery, bindParam);
        }

        public async Task<CDbExecProcResult> Postgres_CALL_(string procedureName, List<CDbQueryParamBind> bindParam = null) {
            return await this.Postgres?.ExecProcedureAsync(procedureName, bindParam);
        }

        public async Task<List<string>> Postgres_RetrieveBlob(string stringPathDownload, string queryString, List<CDbQueryParamBind> bindParam = null, string stringCustomSingleFileName = null) {
            return await this.Postgres?.RetrieveBlob(stringPathDownload, queryString, bindParam, stringCustomSingleFileName);
        }

        public async Task<string> Postgres_BulkGetCsv(string rawQueryVulnerableSqlInjection, string delimiter, string filename, string outputPath = null, bool rawQueryWithoutParam = false, bool doubleQuote = true) {
            return await this.Postgres?.BulkGetCsv(rawQueryVulnerableSqlInjection, delimiter, filename, outputFolderPath: outputPath, useRawQueryWithoutParam: rawQueryWithoutParam, useDoubleQuote: doubleQuote);
        }

        /* ** */

        public async Task<bool> MsSql_TruncateTable(string TableName) {
            return await this.MsSql?.ExecQueryAsync($@"TRUNCATE TABLE {TableName}");
        }

        public async Task<bool> MsSql_BulkInsertInto(string tableName, DataTable dataTable) {
            return await this.MsSql?.BulkInsertInto(tableName, dataTable);
        }

        public async Task<T> MsSql_ExecScalar<T>(string sqlQuery, List<CDbQueryParamBind> bindParam = null) {
            return await this.MsSql?.ExecScalarAsync<T>(sqlQuery, bindParam);
        }

        public async Task<bool> MsSql_ExecQuery(string sqlQuery, List<CDbQueryParamBind> bindParam = null) {
            return await this.MsSql?.ExecQueryAsync(sqlQuery, bindParam);
        }

        public async Task<DbDataReader> MsSql_ExecReaderAsync(string sqlQuery, List<CDbQueryParamBind> bindParam = null) {
            return await this.MsSql?.ExecReaderAsync(sqlQuery, bindParam);
        }

        public async Task<DataTable> MsSql_GetDataTable(string sqlQuery, List<CDbQueryParamBind> bindParam = null) {
            return await this.MsSql?.GetDataTableAsync(sqlQuery, bindParam);
        }

        public async Task<List<T>> MsSql_GetList<T>(string sqlQuery, List<CDbQueryParamBind> bindParam = null) {
            return await this.MsSql?.GetListAsync<T>(sqlQuery, bindParam);
        }

        public async Task<CDbExecProcResult> MsSql_CALL_(string procedureName, List<CDbQueryParamBind> bindParam = null) {
            return await this.MsSql?.ExecProcedureAsync(procedureName, bindParam);
        }

        public async Task<List<string>> MsSql_RetrieveBlob(string stringPathDownload, string queryString, List<CDbQueryParamBind> bindParam = null, string stringCustomSingleFileName = null) {
            return await this.MsSql?.RetrieveBlob(stringPathDownload, queryString, bindParam, stringCustomSingleFileName);
        }

        public async Task<string> MsSql_BulkGetCsv(string rawQueryVulnerableSqlInjection, string delimiter, string filename, string outputPath = null, bool rawQueryWithoutParam = false, bool doubleQuote = true) {
            return await this.MsSql?.BulkGetCsv(rawQueryVulnerableSqlInjection, delimiter, filename, outputFolderPath: outputPath, useRawQueryWithoutParam: rawQueryWithoutParam, useDoubleQuote: doubleQuote);
        }

        /* ** */

        public async Task<bool> MySql_TruncateTable(string TableName) {
            return await this.MySql?.ExecQueryAsync($@"TRUNCATE TABLE {TableName}");
        }

        public async Task<bool> MySql_BulkInsertInto(string tableName, DataTable dataTable) {
            return await this.MySql?.BulkInsertInto(tableName, dataTable);
        }

        public async Task<T> MySql_ExecScalar<T>(string sqlQuery, List<CDbQueryParamBind> bindParam = null) {
            return await this.MySql?.ExecScalarAsync<T>(sqlQuery, bindParam);
        }

        public async Task<bool> MySql_ExecQuery(string sqlQuery, List<CDbQueryParamBind> bindParam = null) {
            return await this.MySql?.ExecQueryAsync(sqlQuery, bindParam);
        }

        public async Task<DbDataReader> MySql_ExecReaderAsync(string sqlQuery, List<CDbQueryParamBind> bindParam = null) {
            return await this.MySql?.ExecReaderAsync(sqlQuery, bindParam);
        }

        public async Task<DataTable> MySql_GetDataTable(string sqlQuery, List<CDbQueryParamBind> bindParam = null) {
            return await this.MySql?.GetDataTableAsync(sqlQuery, bindParam);
        }

        public async Task<List<T>> MySql_GetList<T>(string sqlQuery, List<CDbQueryParamBind> bindParam = null) {
            return await this.MySql?.GetListAsync<T>(sqlQuery, bindParam);
        }

        public async Task<CDbExecProcResult> MySql_CALL_(string procedureName, List<CDbQueryParamBind> bindParam = null) {
            return await this.MySql?.ExecProcedureAsync(procedureName, bindParam);
        }

        public async Task<List<string>> MySql_RetrieveBlob(string stringPathDownload, string queryString, List<CDbQueryParamBind> bindParam = null, string stringCustomSingleFileName = null) {
            return await this.MySql?.RetrieveBlob(stringPathDownload, queryString, bindParam, stringCustomSingleFileName);
        }

        public async Task<string> MySql_BulkGetCsv(string rawQueryVulnerableSqlInjection, string delimiter, string filename, string outputPath = null, bool rawQueryWithoutParam = false, bool doubleQuote = true) {
            return await this.MySql?.BulkGetCsv(rawQueryVulnerableSqlInjection, delimiter, filename, outputFolderPath: outputPath, useRawQueryWithoutParam: rawQueryWithoutParam, useDoubleQuote: doubleQuote);
        }

        /* ** */

        public async Task<bool> SQLite_TruncateTable(string TableName) {
            return await this.Sqlite?.ExecQueryAsync($@"TRUNCATE TABLE {TableName}");
        }

        public async Task<bool> SQLite_BulkInsertInto(string tableName, DataTable dataTable) {
            return await this.Sqlite?.BulkInsertInto(tableName, dataTable);
        }

        public async Task<T> SQLite_ExecScalar<T>(string sqlQuery, List<CDbQueryParamBind> bindParam = null) {
            return await this.Sqlite.ExecScalarAsync<T>(sqlQuery, bindParam);
        }

        public async Task<bool> SQLite_ExecQuery(string sqlQuery, List<CDbQueryParamBind> bindParam = null) {
            return await this.Sqlite.ExecQueryAsync(sqlQuery, bindParam);
        }

        public async Task<DbDataReader> SQLite_ExecReaderAsync(string sqlQuery, List<CDbQueryParamBind> bindParam = null) {
            return await this.Sqlite.ExecReaderAsync(sqlQuery, bindParam);
        }

        public async Task<DataTable> SQLite_GetDataTable(string sqlQuery, List<CDbQueryParamBind> bindParam = null) {
            return await this.Sqlite.GetDataTableAsync(sqlQuery, bindParam);
        }

        public async Task<List<T>> SQLite_GetList<T>(string sqlQuery, List<CDbQueryParamBind> bindParam = null) {
            return await this.Sqlite?.GetListAsync<T>(sqlQuery, bindParam);
        }

        // public async Task<CDbExecProcResult> SQLite_CALL_(string procedureName, List<CDbQueryParamBind> bindParam = null) {
        //     return await this.Sqlite?.ExecProcedureAsync(procedureName, bindParam);
        // }

        public async Task<List<string>> SQLite_RetrieveBlob(string stringPathDownload, string queryString, List<CDbQueryParamBind> bindParam = null, string stringCustomSingleFileName = null) {
            return await this.Sqlite?.RetrieveBlob(stringPathDownload, queryString, bindParam, stringCustomSingleFileName);
        }

        public async Task<string> SQLite_BulkGetCsv(string rawQueryVulnerableSqlInjection, string delimiter, string filename, string outputPath = null, bool rawQueryWithoutParam = false, bool doubleQuote = true) {
            return await this.Sqlite?.BulkGetCsv(rawQueryVulnerableSqlInjection, delimiter, filename, outputFolderPath: outputPath, useRawQueryWithoutParam: rawQueryWithoutParam, useDoubleQuote: doubleQuote);
        }

    }

}
