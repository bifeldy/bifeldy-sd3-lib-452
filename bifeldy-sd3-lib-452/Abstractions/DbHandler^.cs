/**
 * 
 * Author       :: Basilius Bias Astho Christyono
 * Mail         :: bias@indomaret.co.id
 * Phone        :: (+62) 889 236 6466
 * 
 * Department   :: IT SD 03
 * Mail         :: bias@indomaret.co.id
 * 
 * Catatan      :: Kumpulan Handler Database Bawaan
 *              :: Tidak Untuk Didaftarkan Ke DI Container
 *              :: Hanya Untuk Inherit
 * 
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

using bifeldy_sd3_lib_452.Databases;
using bifeldy_sd3_lib_452.Models;
using bifeldy_sd3_lib_452.Utilities;

namespace bifeldy_sd3_lib_452.Abstractions {

    public interface IDbHandler {
        string LoggedInUsername { get; set; }
        string DbName { get; }
        void CloseAllConnection(bool force = false);
        Task MarkBeforeCommitRollback();
        void MarkSuccessCommitAndClose();
        void MarkFailedRollbackAndClose();
        Task<string> GetJenisDc();
        Task<string> GetKodeDc();
        Task<string> GetNamaDc();
        Task<string> CekVersi();
        Task<bool> LoginUser(string usernameNik, string password);
        Task<bool> CheckIpMac();
        Task<bool> TruncateTableOraPg(string TableName);
        Task<bool> TruncateTableMsSql(string TableName);
        Task<bool> BulkInsertIntoOraPg(string tableName, DataTable dataTable);
        Task<bool> BulkInsertIntoMsSql(string tableName, DataTable dataTable);
        COracle NewExternalConnectionOra(string dbIpAddrss, string dbPort, string dbUsername, string dbPassword, string dbNameSid);
        CPostgres NewExternalConnectionPg(string dbIpAddrss, string dbPort, string dbUsername, string dbPassword, string dbName);
        CMsSQL NewExternalConnectionMsSql(string dbIpAddrss, string dbUsername, string dbPassword, string dbName);
    }

    public abstract class CDbHandler : IDbHandler {

        private readonly IApplication _app;

        private readonly IOracle _oracle;
        private readonly IPostgres _postgres;
        private readonly IMsSQL _mssql;

        private string DcCode = null;
        private string DcName = null;
        private string DcJenis = null;

        public string LoggedInUsername { get; set; }

        public CDbHandler(IApplication app, IOracle oracle, IPostgres postgres, IMsSQL mssql) {
            _app = app;

            _oracle = oracle;
            _postgres = postgres;
            _mssql = mssql;
        }

        protected IOracle Oracle {
            get {
                IOracle ret = _oracle.Available ? _oracle : null;
                if (ret == null) {
                    throw new Exception("Gagal Membaca Dan Mengambil `Kunci` Oracle Database");
                }
                return ret;
            }
        }

        protected IPostgres Postgres {
            get {
                IPostgres ret = _postgres.Available ? _postgres : null;
                if (ret == null) {
                    throw new Exception("Gagal Membaca Dan Mengambil `Kunci` Postgres Database");
                }
                return ret;
            }
        }

        protected IMsSQL MsSql {
            get {
                IMsSQL ret = _mssql.Available ? _mssql : null;
                if (ret == null) {
                    throw new Exception("Gagal Membaca Dan Mengambil `Kunci` Ms. SQL Server Database");
                }
                return ret;
            }
        }

        protected IDatabase OraPg {
            get {
                if (_app.IsUsingPostgres) {
                    return Postgres;
                }
                return Oracle;
            }
        }

        /** Custom Queries */

        public string DbName => $"{OraPg?.DbName} / {MsSql?.DbName}";

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

        public async Task<string> GetJenisDc() {
            if (string.IsNullOrEmpty(DcJenis)) {
                DcJenis = await OraPg.ExecScalarAsync<string>("SELECT TBL_JENIS_DC FROM DC_TABEL_DC_T");
            }
            return DcJenis;
        }

        public async Task<string> GetKodeDc() {
            if (string.IsNullOrEmpty(DcCode)) {
                DcCode = await OraPg.ExecScalarAsync<string>("SELECT TBL_DC_KODE FROM DC_TABEL_DC_T");
            }
            return DcCode;
        }

        public async Task<string> GetNamaDc() {
            if (string.IsNullOrEmpty(DcName)) {
                DcName = await OraPg.ExecScalarAsync<string>("SELECT TBL_DC_NAMA FROM DC_TABEL_DC_T");
            }
            return DcName;
        }

        public async Task<string> CekVersi() {
            if (_app.DebugMode) {
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
                                dc_kode = :dc_kode
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
            if (string.IsNullOrEmpty(LoggedInUsername)) {
                LoggedInUsername = await OraPg.ExecScalarAsync<string>(
                    $@"
                        SELECT
                            user_name
                        FROM
                            dc_user_t
                        WHERE
                            (UPPER(user_name) = :user_name OR UPPER(user_nik) = :user_nik)
                            AND UPPER(user_password) = :password
                    ",
                    new List<CDbQueryParamBind> {
                        new CDbQueryParamBind { NAME = "user_name", VALUE = userNameNik },
                        new CDbQueryParamBind { NAME = "user_nik", VALUE = userNameNik },
                        new CDbQueryParamBind { NAME = "password", VALUE = password }
                    }
                );
            }
            return !string.IsNullOrEmpty(LoggedInUsername);
        }

        public async Task<bool> CheckIpMac() {
            if (_app.DebugMode) {
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
                            a.user_name = :user_name AND
                            (b.ip_addr IN (:ip_v4_v6) OR b.ip_addr IN (:mac_addr)) AND
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

        /* Perlakuan Khusus */

        public async Task<bool> TruncateTableOraPg(string TableName) {
            return await OraPg.ExecQueryAsync($@"TRUNCATE TABLE {TableName}");
        }

        public async Task<bool> TruncateTableMsSql(string TableName) {
            return await MsSql.ExecQueryAsync($@"TRUNCATE TABLE {TableName}");
        }

        public async Task<bool> BulkInsertIntoOraPg(string tableName, DataTable dataTable) {
            if (_app.IsUsingPostgres) {
                return await Postgres.BulkInsertInto(tableName, dataTable);
            }
            return await Oracle.BulkInsertInto(tableName, dataTable);
        }

        public async Task<bool> BulkInsertIntoMsSql(string tableName, DataTable dataTable) {
            return await MsSql.BulkInsertInto(tableName, dataTable);
        }

        public COracle NewExternalConnectionOra(string dbIpAddrss, string dbPort, string dbUsername, string dbPassword, string dbNameSid) {
            return Oracle.NewExternalConnection(dbIpAddrss, dbPort, dbUsername, dbPassword, dbNameSid);
        }

        public CPostgres NewExternalConnectionPg(string dbIpAddrss, string dbPort, string dbUsername, string dbPassword, string dbName) {
            return Postgres.NewExternalConnection(dbIpAddrss, dbPort, dbUsername, dbPassword, dbName);
        }

        public CMsSQL NewExternalConnectionMsSql(string dbIpAddrss, string dbUsername, string dbPassword, string dbName) {
            return MsSql.NewExternalConnection(dbIpAddrss, dbUsername, dbPassword, dbName);
        }

    }

}
