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
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

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
        void MarkFailedRollbackAndClose();
        COracle NewExternalConnectionOra(string dbIpAddrss, string dbPort, string dbUsername, string dbPassword, string dbNameSid);
        CPostgres NewExternalConnectionPg(string dbIpAddrss, string dbPort, string dbUsername, string dbPassword, string dbName);
        CMsSQL NewExternalConnectionMsSql(string dbIpAddrss, string dbUsername, string dbPassword, string dbName);
        Task<string> GetJenisDc();
        Task<string> GetKodeDc();
        Task<string> GetNamaDc();
        Task<string> CekVersi();
        Task<bool> LoginUser(string usernameNik, string password);
        Task<bool> CheckIpMac();
        Task<string> OraPg_GetURLWebService(string webType);
        Task<T> OraPg_GetMailInfo<T>(string kolom);
        Task<DateTime> OraPg_GetYesterdayDate(int lastDay);
        Task<DateTime> OraPg_GetLastMonth(int lastMonth);
        Task<DateTime> OraPg_GetCurrentTimestamp();
        Task<DateTime> OraPg_GetCurrentDate();
        Task<bool> OraPg_AlterTable_AddColumnIfNotExist(string tableName, string columnName, string columnType);
        Task<bool> OraPg_TruncateTable(string TableName);
        Task<bool> OraPg_BulkInsertInto(string tableName, DataTable dataTable);
        Task<T> OraPg_ExecScalar<T>(string sqlQuery, List<CDbQueryParamBind> bindParam = null);
        Task<bool> OraPg_ExecQuery(string sqlQuery, List<CDbQueryParamBind> bindParam = null);
        Task<DataTable> OraPg_GetDataTable(string sqlQuery, List<CDbQueryParamBind> bindParam = null);
        Task<CDbExecProcResult> OraPg_CALL_(string procName, List<CDbQueryParamBind> bindParam = null);
        Task<T> MsSql_ExecScalar<T>(string sqlQuery, List<CDbQueryParamBind> bindParam = null);
        Task<bool> MsSql_ExecQuery(string sqlQuery, List<CDbQueryParamBind> bindParam = null);
        Task<DataTable> MsSql_GetDataTable(string sqlQuery, List<CDbQueryParamBind> bindParam = null);
        Task<CDbExecProcResult> MsSql_CALL_(string procedureName, List<CDbQueryParamBind> bindParam = null);
        Task<T> SQLite_ExecScalar<T>(string sqlQuery, List<CDbQueryParamBind> bindParam = null);
        Task<bool> SQLite_ExecQuery(string sqlQuery, List<CDbQueryParamBind> bindParam = null);
        Task<DataTable> SQLite_GetDataTable(string sqlQuery, List<CDbQueryParamBind> bindParam = null);
    }

    public class CDbHandler : IDbHandler {

        public bool LocalDbOnly { get; }

        private readonly IApplication _app;

        private readonly IOracle _oracle;
        private readonly IPostgres _postgres;
        private readonly IMsSQL _mssql;
        private readonly ISqlite _sqlite;

        private string DcCode = null;
        private string DcName = null;
        private string DcJenis = null;

        public string LoggedInUsername { get; set; }

        public CDbHandler(IApplication app, IConfig config, IOracle oracle, IPostgres postgres, IMsSQL mssql, ISqlite sqlite) {
            LocalDbOnly = config.Get<bool>("LocalDbOnly", bool.Parse(app.GetConfig("local_db_only")));

            _app = app;
            _oracle = oracle;
            _postgres = postgres;
            _mssql = mssql;
            _sqlite = sqlite;
        }

        protected IOracle Oracle {
            get {
                if (LocalDbOnly) {
                    throw new Exception("Hanya Bisa Menggunakan SQLite Dalam Mode Lokal Database Saja");
                }
                IOracle ret = _oracle.Available ? _oracle : null;
                if (ret == null) {
                    throw new Exception("Gagal Membaca Dan Mengambil `Kunci` Oracle Database");
                }
                return ret;
            }
        }

        protected IPostgres Postgres {
            get {
                if (LocalDbOnly) {
                    throw new Exception("Hanya Bisa Menggunakan SQLite Dalam Mode Lokal Database Saja");
                }
                IPostgres ret = _postgres.Available ? _postgres : null;
                if (ret == null) {
                    throw new Exception("Gagal Membaca Dan Mengambil `Kunci` Postgres Database");
                }
                return ret;
            }
        }

        protected IDatabase OraPg {
            get {
                if (LocalDbOnly) {
                    throw new Exception("Hanya Bisa Menggunakan SQLite Dalam Mode Lokal Database Saja");
                }
                if (_app.IsUsingPostgres) {
                    return Postgres;
                }
                return Oracle;
            }
        }

        protected IMsSQL MsSql {
            get {
                if (LocalDbOnly) {
                    throw new Exception("Hanya Bisa Menggunakan SQLite Dalam Mode Lokal Database Saja");
                }
                IMsSQL ret = _mssql.Available ? _mssql : null;
                if (ret == null) {
                    throw new Exception("Gagal Membaca Dan Mengambil `Kunci` Ms. SQL Server Database");
                }
                return ret;
            }
        }

        protected ISqlite Sqlite {
            get {
                ISqlite ret = _sqlite.Available ? _sqlite : null;
                if (ret == null) {
                    throw new Exception("Gagal Membaca Dan Mengambil `Kunci` SQLite Database");
                }
                return ret;
            }
        }

        /** Custom Queries */

        public string DbName {
            get {
                if (LocalDbOnly) {
                    return Sqlite.DbName?.Replace("\\", "/").Split('/').Last();
                }
                string FullDbName = string.Empty;
                try {
                    FullDbName += OraPg.DbName;
                }
                catch {
                    FullDbName += "-";
                }
                FullDbName += " / ";
                try {
                    FullDbName += MsSql.DbName;
                }
                catch {
                    FullDbName += "-";
                }
                return FullDbName;
            }
        }

        public string GetAllAvailableDbConnectionsString() {
            // Bypass Check DB Availablility ~
            string oracle = $"Oracle :: {_oracle.DbName}\r\n\r\n{_oracle.DbConnectionString}\r\n\r\n\r\n";
            string postgre = $"Postgres :: {_postgres.DbName}\r\n\r\n{_postgres.DbConnectionString}\r\n\r\n\r\n";
            string mssql = $"Postgres :: {_mssql.DbName}\r\n\r\n{_mssql.DbConnectionString}\r\n\r\n\r\n";
            string sqlite = $"SQLite :: {_sqlite.DbName?.Replace("\\", "/").Split('/').Last()}\r\n\r\n{_sqlite.DbConnectionString}";
            return oracle + postgre + mssql + sqlite;
        }

        public void CloseAllConnection(bool force = false) {
            _oracle.CloseConnection(force);
            _postgres.CloseConnection(force);
            _mssql.CloseConnection(force);
        }

        public async Task MarkBeforeCommitRollback() {
            await _oracle.MarkBeforeCommitRollback();
            await _postgres.MarkBeforeCommitRollback();
            await _mssql.MarkBeforeCommitRollback();
        }

        public void MarkSuccessCommitAndClose() {
            _oracle.MarkSuccessCommitAndClose();
            _postgres.MarkSuccessCommitAndClose();
            _mssql.MarkSuccessCommitAndClose();
        }

        public void MarkFailedRollbackAndClose() {
            _oracle.MarkFailedRollbackAndClose();
            _postgres.MarkFailedRollbackAndClose();
            _mssql.MarkFailedRollbackAndClose();
        }

        /* Perlakuan Khusus */

        public COracle NewExternalConnectionOra(string dbIpAddrss, string dbPort, string dbUsername, string dbPassword, string dbNameSid) {
            return _oracle.NewExternalConnection(dbIpAddrss, dbPort, dbUsername, dbPassword, dbNameSid);
        }

        public CPostgres NewExternalConnectionPg(string dbIpAddrss, string dbPort, string dbUsername, string dbPassword, string dbName) {
            return _postgres.NewExternalConnection(dbIpAddrss, dbPort, dbUsername, dbPassword, dbName);
        }

        public CMsSQL NewExternalConnectionMsSql(string dbIpAddrss, string dbUsername, string dbPassword, string dbName) {
            return _mssql.NewExternalConnection(dbIpAddrss, dbUsername, dbPassword, dbName);
        }

        /* ** */

        public async Task<string> GetJenisDc() {
            if (LocalDbOnly) {
                return "NONDC";
            }
            if (OraPg.DbUsername.ToUpper().Contains("DCHO")) {
                return "HO";
            }
            if (string.IsNullOrEmpty(DcJenis)) {
                DcJenis = await OraPg.ExecScalarAsync<string>("SELECT TBL_JENIS_DC FROM DC_TABEL_DC_T");
            }
            return DcJenis.ToUpper();
        }

        public async Task<string> GetKodeDc() {
            if (LocalDbOnly) {
                return "GXXX";
            }
            if (OraPg.DbUsername.ToUpper().Contains("DCHO")) {
                return "DCHO";
            }
            if (string.IsNullOrEmpty(DcCode)) {
                DcCode = await OraPg.ExecScalarAsync<string>("SELECT TBL_DC_KODE FROM DC_TABEL_DC_T");
            }
            return DcCode.ToUpper();
        }

        public async Task<string> GetNamaDc() {
            if (LocalDbOnly) {
                return "NON DC";
            }
            if (OraPg.DbUsername.ToUpper().Contains("DCHO")) {
                return "DC HEAD OFFICE";
            }
            if (string.IsNullOrEmpty(DcName)) {
                DcName = await OraPg.ExecScalarAsync<string>("SELECT TBL_DC_NAMA FROM DC_TABEL_DC_T");
            }
            return DcName.ToUpper();
        }

        public async Task<string> CekVersi() {
            if (_app.DebugMode || LocalDbOnly) {
                return "OKE";
            }
            else {
                try {
                    string res1 = await OraPg.ExecScalarAsync<string>(
                        $@"
                            SELECT
                                CASE
                                    WHEN COALESCE(aprove, 'N') = 'Y' AND {(
                                            _app.IsUsingPostgres ?
                                            "COALESCE(tgl_berlaku, NOW())::DATE <= CURRENT_DATE" :
                                            "TRUNC(COALESCE(tgl_berlaku, SYSDATE)) <= TRUNC(SYSDATE)"
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
                            new CDbQueryParamBind { NAME = "dc_kode", VALUE = await GetKodeDc() },
                            new CDbQueryParamBind { NAME = "nama_prog", VALUE = $"%{_app.AppName}%" }
                        }
                    );
                    if (string.IsNullOrEmpty(res1)) {
                        return $"Program :: {_app.AppName}" + Environment.NewLine + "Belum Terdaftar Di Master Program DC";
                    }
                    if (res1 == _app.AppVersion) {
                        try {
                            bool res2 = await OraPg.ExecQueryAsync(
                                $@"
                                    INSERT INTO dc_monitoring_program_t (kode_dc, nama_program, ip_client, versi, tanggal)
                                    VALUES (:kode_dc, :nama_program, :ip_client, :versi, {(_app.IsUsingPostgres ? "NOW()" : "SYSDATE")})
                                ",
                                new List<CDbQueryParamBind> {
                                    new CDbQueryParamBind { NAME = "kode_dc", VALUE = await GetKodeDc() },
                                    new CDbQueryParamBind { NAME = "nama_program", VALUE = _app.AppName },
                                    new CDbQueryParamBind { NAME = "ip_client", VALUE = _app.GetAllIpAddress().FirstOrDefault() },
                                    new CDbQueryParamBind { NAME = "versi", VALUE = _app.AppVersion }
                                }
                            );
                            return "OKE";
                        }
                        catch (Exception ex2) {
                            return ex2.Message;
                        }
                    }
                    else {
                        return $"Versi Program :: {_app.AppName}" + Environment.NewLine + $"Tidak Sama Dengan Master Program = v{res1}";
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
                    {(LocalDbOnly ? "uname" : "user_name")}
                FROM
                    {(LocalDbOnly ? "users" : "dc_user_t")}
                WHERE
                    {(LocalDbOnly ? @"
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
            if (string.IsNullOrEmpty(LoggedInUsername)) {
                if (LocalDbOnly) {
                    byte[] pswd = new SHA1Managed().ComputeHash(Encoding.UTF8.GetBytes(password));
                    string hash = string.Concat(pswd.Select(b => b.ToString("x2")));
                    param.Add(new CDbQueryParamBind { NAME = "pass", VALUE = hash });
                    LoggedInUsername = await Sqlite.ExecScalarAsync<string>(query, param);
                }
                else {
                    param.Add(new CDbQueryParamBind { NAME = "unik", VALUE = userNameNik });
                    param.Add(new CDbQueryParamBind { NAME = "pass", VALUE = password });
                    LoggedInUsername = await OraPg.ExecScalarAsync<string>(query, param);
                }
            }
            return !string.IsNullOrEmpty(LoggedInUsername);
        }

        public async Task<bool> CheckIpMac() {
            if (_app.DebugMode || LocalDbOnly) {
                return true;
            }
            else {
                string res = await OraPg.ExecScalarAsync<string>(
                    $@"
                        SELECT
                            a.user_name
                        FROM
                            dc_user_t a,
                            dc_ipaddress_t b,
                            dc_security_t c
                        WHERE
                            a.user_name = b.ip_fk_user_name AND
                            a.user_name = c.fk_user_name AND
                            UPPER(a.user_name) = UPPER(:user_name) AND
                            (UPPER(b.ip_addr) IN (:ip_v4_v6) OR UPPER(b.ip_addr) IN (:mac_addr)) AND
                            UPPER(c.sec_app_name) LIKE :app_name
                    ",
                    new List<CDbQueryParamBind> {
                        new CDbQueryParamBind { NAME = "user_name", VALUE = LoggedInUsername },
                        new CDbQueryParamBind { NAME = "ip_v4_v6", VALUE = _app.GetAllIpAddress() },
                        new CDbQueryParamBind { NAME = "mac_addr", VALUE = _app.GetAllMacAddress() },
                        new CDbQueryParamBind { NAME = "app_name", VALUE = $"%{_app.AppName}%" }
                    }
                );
                return (res == LoggedInUsername);
            }
        }

        public async Task<string> OraPg_GetURLWebService(string webType) {
            return await OraPg.ExecScalarAsync<string>(
                $@"SELECT WEB_URL FROM DC_WEBSERVICE_T WHERE WEB_TYPE = :web_type",
                new List<CDbQueryParamBind> {
                    new CDbQueryParamBind { NAME = "web_type", VALUE = webType }
                }
            );
        }

        public async Task<T> OraPg_GetMailInfo<T>(string kolom) {
            return await OraPg.ExecScalarAsync<T>(
                $@"SELECT {kolom} FROM DC_LISTMAILSERVER_T WHERE MAIL_DCKODE = :mail_dckode",
                new List<CDbQueryParamBind> {
                    new CDbQueryParamBind { NAME = "mail_dckode", VALUE = await GetKodeDc() }
                }
            );
        }

        public async Task<DateTime> OraPg_GetYesterdayDate(int lastDay) {
            return await OraPg.ExecScalarAsync<DateTime>(
                $@"
                    SELECT {(_app.IsUsingPostgres ? "CURRENT_DATE" : "TRUNC(SYSDATE)")} - :last_day
                    {(_app.IsUsingPostgres ? "" : "FROM DUAL")}
                ",
                new List<CDbQueryParamBind> {
                    new CDbQueryParamBind { NAME = "last_day", VALUE = lastDay }
                }
            );
        }

        public async Task<DateTime> OraPg_GetLastMonth(int lastMonth) {
            return await OraPg.ExecScalarAsync<DateTime>(
                $@"
                    SELECT TRUNC(add_months({(_app.IsUsingPostgres ? "CURRENT_DATE" : "SYSDATE")}, - :last_month))
                    {(_app.IsUsingPostgres ? "" : "FROM DUAL")}
                ",
                new List<CDbQueryParamBind> {
                    new CDbQueryParamBind { NAME = "last_month", VALUE = lastMonth }
                }
            );
        }

        public async Task<DateTime> OraPg_GetCurrentTimestamp() {
            return await OraPg.ExecScalarAsync<DateTime>($@"
                SELECT {(_app.IsUsingPostgres ? "CURRENT_TIMESTAMP" : "SYSDATE FROM DUAL")}
            ");
        }

        public async Task<DateTime> OraPg_GetCurrentDate() {
            return await OraPg.ExecScalarAsync<DateTime>($@"
                SELECT {(_app.IsUsingPostgres ? "CURRENT_DATE" : "TRUNC(SYSDATE) FROM DUAL")}
            ");
        }

        /* ** */

        // Bisa Kena SQL Injection
        public async Task<bool> OraPg_AlterTable_AddColumnIfNotExist(string tableName, string columnName, string columnType) {
            List<string> cols = new List<string>();
            DataColumnCollection columns = await OraPg.GetAllColumnTableAsync(tableName);
            foreach (DataColumn col in columns) {
                cols.Add(col.ColumnName.ToUpper());
            }
            if (!cols.Contains(columnName.ToUpper())) {
                return await OraPg.ExecQueryAsync($@"
                    ALTER TABLE {tableName}
                        ADD {(_app.IsUsingPostgres ? "COLUMN" : "(")}
                            {columnName} {columnType}
                        {(_app.IsUsingPostgres ? "" : ")")}
                ");
            }
            return false;
        }

        public async Task<bool> OraPg_TruncateTable(string TableName) {
            return await OraPg.ExecQueryAsync($@"TRUNCATE TABLE {TableName}");
        }

        public async Task<bool> OraPg_BulkInsertInto(string tableName, DataTable dataTable) {
            return await OraPg.BulkInsertInto(tableName, dataTable);
        }

        public async Task<T> OraPg_ExecScalar<T>(string sqlQuery, List<CDbQueryParamBind> bindParam = null) {
            return await OraPg.ExecScalarAsync<T>(sqlQuery, bindParam);
        }

        public async Task<bool> OraPg_ExecQuery(string sqlQuery, List<CDbQueryParamBind> bindParam = null) {
            return await OraPg.ExecQueryAsync(sqlQuery, bindParam);
        }

        public async Task<DataTable> OraPg_GetDataTable(string sqlQuery, List<CDbQueryParamBind> bindParam = null) {
            return await OraPg.GetDataTableAsync(sqlQuery, bindParam);
        }

        public async Task<CDbExecProcResult> OraPg_CALL_(string procedureName, List<CDbQueryParamBind> bindParam = null) {
            return await OraPg.ExecProcedureAsync(procedureName, bindParam);
        }

        /* ** */

        public async Task<T> MsSql_ExecScalar<T>(string sqlQuery, List<CDbQueryParamBind> bindParam = null) {
            return await MsSql.ExecScalarAsync<T>(sqlQuery, bindParam);
        }

        public async Task<bool> MsSql_ExecQuery(string sqlQuery, List<CDbQueryParamBind> bindParam = null) {
            return await MsSql.ExecQueryAsync(sqlQuery, bindParam);
        }

        public async Task<DataTable> MsSql_GetDataTable(string sqlQuery, List<CDbQueryParamBind> bindParam = null) {
            return await MsSql.GetDataTableAsync(sqlQuery, bindParam);
        }

        public async Task<CDbExecProcResult> MsSql_CALL_(string procedureName, List<CDbQueryParamBind> bindParam = null) {
            return await MsSql.ExecProcedureAsync(procedureName, bindParam);
        }

        /* ** */

        public async Task<T> SQLite_ExecScalar<T>(string sqlQuery, List<CDbQueryParamBind> bindParam = null) {
            return await Sqlite.ExecScalarAsync<T>(sqlQuery, bindParam);
        }

        public async Task<bool> SQLite_ExecQuery(string sqlQuery, List<CDbQueryParamBind> bindParam = null) {
            return await Sqlite.ExecQueryAsync(sqlQuery, bindParam);
        }

        public async Task<DataTable> SQLite_GetDataTable(string sqlQuery, List<CDbQueryParamBind> bindParam = null) {
            return await Sqlite.GetDataTableAsync(sqlQuery, bindParam);
        }

    }

}
