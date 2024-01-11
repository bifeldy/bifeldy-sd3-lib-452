/**
 * 
 * Author       :: Basilius Bias Astho Christyono
 * Phone        :: (+62) 889 236 6466
 * 
 * Department   :: IT SD 03
 * Mail         :: bias@indomaret.co.id
 * 
 * Catatan      :: Branch Connection Induk & Cabangnya
 *              :: Harap Didaftarkan Ke DI Container
 * 
 */

using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;

using bifeldy_sd3_lib_452.Abstractions;
using bifeldy_sd3_lib_452.Models;
using bifeldy_sd3_lib_452.Utilities;

namespace bifeldy_sd3_lib_452.Handlers {

    public interface IBranchCabangHandler {
        Task<List<DC_TABEL_V>> GetListBranchDbInformation(string kodeDcInduk);
        Task<IDictionary<string, CDatabase>> GetListBranchDbConnection(string kodeDcInduk);
    }

    public class CBranchCabangHandler : IBranchCabangHandler {

        private readonly IApplication _app;
        private readonly IConfig _config;
        private readonly IApi _api;
        private readonly IDbHandler _db;
        private readonly ILogger _logger;
        private readonly IConverter _converter;

        private IDictionary<
            string, IDictionary<
                string, CDatabase
            >
        > BranchConnectionInfo { get; } = new Dictionary<
            string, IDictionary<
                string, CDatabase
            >
        >();

        public CBranchCabangHandler(IApplication app, IConfig config, IApi api, IDbHandler db, ILogger logger, IConverter converter) {
            _app = app;
            _config = config;
            _api = api;
            _db = db;
            _logger = logger;
            _converter = converter;
        }

        public async Task<List<DC_TABEL_V>> GetListBranchDbInformation(string kodeDcInduk) {
            string url = await _db.OraPg_GetURLWebService("SYNCHO") ?? _config.Get<string>("WsSyncHo", _app.GetConfig("ws_syncho"));
            url += kodeDcInduk;

            HttpResponseMessage httpResponse = await _api.PostData(url, null);
            string httpResString = await httpResponse.Content.ReadAsStringAsync();

            return _converter.JsonToObject<List<DC_TABEL_V>>(httpResString);
        }

        //
        // Sepertinya Yang Ini Akan Kurang Berguna
        // Karena Dapat Akses Langsung Ke Database
        // Cuma Tahu `CDatabase` Tidak Tahu Jenis `Postgre` / `Oracle`
        //
        // IDictionary<string, CDatabase> dbCon = await GetListBranchDbConnection("G001");
        // var res = dbCon["G055"].ExecScalarAsync<...>(...);
        //

        public async Task<IDictionary<string, CDatabase>> GetListBranchDbConnection(string kodeDcInduk) {
            IDictionary<string, CDatabase> dbCons = new Dictionary<string, CDatabase>();

            try {
                List<DC_TABEL_V> dbInfo = await GetListBranchDbInformation(kodeDcInduk);
                foreach (DC_TABEL_V dbi in dbInfo) {
                    CDatabase dbCon;
                    if (dbi.FLAG_DBPG == "Y") {
                        dbCon = _db.NewExternalConnectionPg(dbi.DBPG_IP, dbi.DBPG_PORT, dbi.DBPG_USER, dbi.DBPG_PASS, dbi.DBPG_NAME);
                    }
                    else {
                        dbCon = _db.NewExternalConnectionOra(dbi.IP_DB, dbi.DB_PORT, dbi.DB_USER_NAME, dbi.DB_PASSWORD, dbi.DB_SID);
                    }
                    dbCons.Add(dbi.TBL_DC_KODE, dbCon);
                }
                BranchConnectionInfo[kodeDcInduk] = dbCons;
            }
            catch (Exception ex) {
                _logger.WriteError(ex);
            }

            return BranchConnectionInfo[kodeDcInduk];
        }

    }

}
