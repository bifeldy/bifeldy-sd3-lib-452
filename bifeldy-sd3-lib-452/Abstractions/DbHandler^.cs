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
using System.Linq;
using System.Threading.Tasks;

using bifeldy_sd3_lib_452.Databases;
using bifeldy_sd3_lib_452.Models;
using bifeldy_sd3_lib_452.Utilities;

namespace bifeldy_sd3_lib_452.Abstractions {

    public interface IDbHandler {
        string LoggedInUsername { get; set; }
        string DbName { get; }
        IOracle Oracle { get; }
        IPostgres Postgres { get; }
        IMsSQL MsSql { get; }
        IDatabase OraPg { get; }
        Task<string> GetJenisDc();
        Task<string> GetKodeDc();
        Task<string> GetNamaDc();
        Task<string> CekVersi();
        Task<bool> LoginUser(string usernameNik, string password);
        Task<bool> CheckIpMac();
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

        public IOracle Oracle {
            get {
                IOracle ret = _oracle.Available ? _oracle : null;
                if (ret == null) {
                    throw new Exception("Gagal Membaca Dan Mengambil `Kunci` Oracle Database");
                }
                return ret;
            }
        }

        public IPostgres Postgres {
            get {
                IPostgres ret = _postgres.Available ? _postgres : null;
                if (ret == null) {
                    throw new Exception("Gagal Membaca Dan Mengambil `Kunci` Postgres Database");
                }
                return ret;
            }
        }

        public IMsSQL MsSql {
            get {
                IMsSQL ret = _mssql.Available ? _mssql : null;
                if (ret == null) {
                    throw new Exception("Gagal Membaca Dan Mengambil `Kunci` Ms. SQL Server Database");
                }
                return ret;
            }
        }

        public IDatabase OraPg {
            get {
                if (_app.IsUsingPostgres) {
                    return Postgres;
                }
                return Oracle;
            }
        }

        /** Custom Queries */

        public string DbName => $"{OraPg?.DbName} / {MsSql?.DbName}";

        public async Task<string> GetJenisDc() {
            if (string.IsNullOrEmpty(DcJenis)) {
                (string res, Exception ex) = await OraPg.ExecScalarAsync<string>("SELECT TBL_JENIS_DC FROM DC_TABEL_DC_T");
                DcJenis = (ex == null) ? res : throw ex;
            }
            return DcJenis;
        }

        public async Task<string> GetKodeDc() {
            if (string.IsNullOrEmpty(DcCode)) {
                (string res, Exception ex) = await OraPg.ExecScalarAsync<string>("SELECT TBL_DC_KODE FROM DC_TABEL_DC_T");
                DcCode = (ex == null) ? res : throw ex;
            }
            return DcCode;
        }

        public async Task<string> GetNamaDc() {
            if (string.IsNullOrEmpty(DcName)) {
                (string res, Exception ex) = await OraPg.ExecScalarAsync<string>("SELECT TBL_DC_NAMA FROM DC_TABEL_DC_T");
                DcName = (ex == null) ? res : throw ex;
            }
            return DcName;
        }

        public async Task<string> CekVersi() {
            bool debug = false;
            #if DEBUG
                debug = true;
            #endif
            if (debug) {
                return "OKE";
            }
            else {
                (string res1, Exception ex1) = await OraPg.ExecScalarAsync<string>(
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
                if (ex1 == null) {
                    if (string.IsNullOrEmpty(res1)) {
                        return $"Program :: {_app.AppName}" + Environment.NewLine + "Belum Terdaftar Di Master Program DC";
                    }
                    if (res1 == _app.AppVersion) {
                        (bool res2, Exception ex2) = await OraPg.ExecQueryAsync(
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
                        return res2 ? "OKE" : ex2.Message;
                    }
                    else {
                        return $"Versi Program :: {_app.AppName}" + Environment.NewLine + $"Tidak Sama Dengan Master Program = v{res1}";
                    }
                }
                return ex1.Message;
            }
        }

        public async Task<bool> LoginUser(string userNameNik, string password) {
            if (string.IsNullOrEmpty(LoggedInUsername)) {
                (string res, Exception ex) = await OraPg.ExecScalarAsync<string>(
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
                LoggedInUsername = (ex == null) ? res : throw ex;
            }
            return !string.IsNullOrEmpty(LoggedInUsername);
        }

        public async Task<bool> CheckIpMac() {
            bool debug = false;
            #if DEBUG
                debug = true;
            #endif
            if (debug) {
                return true;
            }
            else {
                (string res, Exception ex) = await OraPg.ExecScalarAsync<string>(
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
                return (ex == null) ? (res == LoggedInUsername) : throw ex;
            }
        }

    }

}
