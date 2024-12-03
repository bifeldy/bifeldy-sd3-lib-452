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

using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;

using bifeldy_sd3_lib_452.Abstractions;
using bifeldy_sd3_lib_452.TableView;
using bifeldy_sd3_lib_452.Utilities;

namespace bifeldy_sd3_lib_452.Handlers {

    public interface IBranchCabangHandler {
        Task<List<DC_TABEL_V>> GetListBranchDbInformation(string kodeDcInduk);
        Task<IDictionary<string, (bool, CDatabase)>> GetListBranchDbConnection(string kodeDcInduk);
    }

    public class CBranchCabangHandler : IBranchCabangHandler {

        private readonly IApplication _app;
        private readonly IConfig _config;
        private readonly IApi _api;
        private readonly IDbHandler _db;
        private readonly IConverter _converter;

        private IDictionary<
            string, IDictionary<
                string, (bool, CDatabase)
            >
        > BranchConnectionInfo { get; } = new Dictionary<
            string, IDictionary<
                string, (bool, CDatabase)
            >
        >();

        public CBranchCabangHandler(IApplication app, IConfig config, IApi api, IDbHandler db, IConverter converter) {
            this._app = app;
            this._config = config;
            this._api = api;
            this._db = db;
            this._converter = converter;
        }

        public async Task<List<DC_TABEL_V>> GetListBranchDbInformation(string kodeDcInduk) {
            string url = await this._db.OraPg_GetURLWebService("SYNCHO") ?? this._config.Get<string>("WsSyncHo", this._app.GetConfig("ws_syncho"));
            url += kodeDcInduk;

            HttpResponseMessage httpResponse = await this._api.PostData(url, null);
            string httpResString = await httpResponse.Content.ReadAsStringAsync();

            return this._converter.JsonToObject<List<DC_TABEL_V>>(httpResString);
        }

        //
        // Akses Langsung Ke Database Cabang
        // Tembak Ambil Info Dari Service Mas Edwin :) HO
        // Atur URL Di `App.config` -> ws_syncho
        //
        // Item1 => bool :: Apakah Menggunakan Postgre
        // Item2 => CDatabase :: Koneksi Ke Database Oracle / Postgre (Tidak Ada SqlServer)
        //
        // IDictionary<string, (bool, CDatabase)> dbCon = await GetListBranchDbConnection("G001");
        // var res = dbCon["G055"].Item2.ExecScalarAsync<...>(...);
        //
        public async Task<IDictionary<string, (bool, CDatabase)>> GetListBranchDbConnection(string kodeDcInduk) {
            if (!this.BranchConnectionInfo.ContainsKey(kodeDcInduk)) {
                IDictionary<string, (bool, CDatabase)> dbCons = new Dictionary<string, (bool, CDatabase)>();

                List<DC_TABEL_V> dbInfo = await this.GetListBranchDbInformation(kodeDcInduk);
                foreach (DC_TABEL_V dbi in dbInfo) {
                    CDatabase dbCon;
                    bool isPostgre = dbi.FLAG_DBPG?.ToUpper() == "Y";
                    if (isPostgre) {
                        dbCon = this._db.NewExternalConnectionPg(dbi.DBPG_IP, dbi.DBPG_PORT, dbi.DBPG_USER, dbi.DBPG_PASS, dbi.DBPG_NAME);
                    }
                    else {
                        dbCon = this._db.NewExternalConnectionOra(dbi.IP_DB, dbi.DB_PORT, dbi.DB_USER_NAME, dbi.DB_PASSWORD, dbi.DB_SID);
                    }

                    dbCons.Add(dbi.TBL_DC_KODE, (isPostgre, dbCon));
                }

                this.BranchConnectionInfo[kodeDcInduk] = dbCons;
            }

            return this.BranchConnectionInfo[kodeDcInduk];
        }

    }

}
