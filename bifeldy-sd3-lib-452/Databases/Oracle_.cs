/**
 * 
 * Author       :: Basilius Bias Astho Christyono
 * Phone        :: (+62) 889 236 6466
 * 
 * Department   :: IT SD 03
 * Mail         :: bias@indomaret.co.id
 * 
 * Catatan      :: Turunan `CDatabase`
 *              :: Harap Didaftarkan Ke DI Container
 *              :: Instance Oracle
 * 
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

using Oracle.ManagedDataAccess.Client;

using bifeldy_sd3_lib_452.Abstractions;
using bifeldy_sd3_lib_452.Models;
using bifeldy_sd3_lib_452.Utilities;

namespace bifeldy_sd3_lib_452.Databases {

    public interface IOracle : IDatabase {
        COracle NewExternalConnection(string dbIpAddrss, string dbPort, string dbUsername, string dbPassword, string dbNameSid);
    }

    public sealed class COracle : CDatabase, IOracle {

        private readonly IApplication _app;
        private readonly ILogger _logger;

        private OracleCommand DatabaseCommand { get; set; }
        private OracleDataAdapter DatabaseAdapter { get; set; }

        public COracle(IApplication app, ILogger logger, IConverter converter) : base(logger, converter) {
            _app = app;
            _logger = logger;

            InitializeOracleDatabase();
            SettingUpDatabase();
        }

        private void InitializeOracleDatabase(string dbUsername = null, string dbPassword = null, string dbTnsOdp = null) {
            DbUsername = dbUsername ?? _app.GetVariabel("UserOrcl");
            DbPassword = dbPassword ?? _app.GetVariabel("PasswordOrcl");
            string _dbTnsOdp = dbTnsOdp ?? _app.GetVariabel("ODPOrcl");
            if (!string.IsNullOrEmpty(_dbTnsOdp)) {
                _dbTnsOdp = Regex.Replace(_dbTnsOdp, @"\s+", "");
            }
            DbTnsOdp = dbTnsOdp ?? _dbTnsOdp;
        }

        private void SettingUpDatabase() {
            string _dbName = null;
            if (!string.IsNullOrEmpty(DbTnsOdp)) {
                _dbName = DbTnsOdp.Split(new string[] { "SERVICE_NAME=" }, StringSplitOptions.None)[1].Split(new string[] { ")" }, StringSplitOptions.None)[0];
            }
            DbName = _dbName;
            try {
                DbConnectionString = $"Data Source={DbTnsOdp};User ID={DbUsername};Password={DbPassword};Connection Timeout=180;"; // 3 menit
                if (
                    string.IsNullOrEmpty(DbTnsOdp) ||
                    string.IsNullOrEmpty(DbUsername) ||
                    string.IsNullOrEmpty(DbPassword)
                ) {
                    throw new Exception("Database Not Available!");
                }
                DatabaseConnection = new OracleConnection(DbConnectionString);
                DatabaseCommand = new OracleCommand {
                    Connection = (OracleConnection)DatabaseConnection,
                    BindByName = true,
                    InitialLOBFetchSize = -1,
                    InitialLONGFetchSize = -1,
                    CommandTimeout = 1800 // 30 menit
                };
                DatabaseAdapter = new OracleDataAdapter(DatabaseCommand);
                _logger.WriteInfo(GetType().Name, DbConnectionString);
            }
            catch (Exception ex) {
                _logger.WriteError(ex);
            }
        }

        private void BindQueryParameter(List<CDbQueryParamBind> parameters) {
            DatabaseCommand.Parameters.Clear();
            if (parameters != null) {
                for (int i = 0; i < parameters.Count; i++) {
                    dynamic pVal = parameters[i].VALUE;
                    Type pValType = (pVal == null) ? typeof(DBNull) : pVal.GetType();
                    if (pValType.IsArray) {
                        string bindStr = string.Empty;
                        int id = 1;
                        foreach (dynamic data in pVal) {
                            if (!string.IsNullOrEmpty(bindStr)) {
                                bindStr += ", ";
                            }
                            bindStr += $":{parameters[i].NAME}_{id}";
                            DatabaseCommand.Parameters.Add(new OracleParameter {
                                ParameterName = $"{parameters[i].NAME}_{id}",
                                Value = data ?? DBNull.Value
                            });
                            id++;
                        }
                        Regex regex = new Regex($":{parameters[i].NAME}");
                        DatabaseCommand.CommandText = regex.Replace(DatabaseCommand.CommandText, bindStr, 1);
                    }
                    else {
                        OracleParameter param = new OracleParameter {
                            ParameterName = parameters[i].NAME,
                            Value = pVal ?? DBNull.Value
                        };
                        if (parameters[i].SIZE > 0) {
                            param.Size = parameters[i].SIZE;
                        }
                        if (parameters[i].DIRECTION > 0) {
                            param.Direction = parameters[i].DIRECTION;
                        }
                        DatabaseCommand.Parameters.Add(param);
                    }
                }
            }
            LogQueryParameter(DatabaseCommand);
        }

        /** Bagian Ini Mirip :: Oracle - Ms. Sql Server - PostgreSQL */

        public override async Task<DataColumnCollection> GetAllColumnTableAsync(string tableName) {
            DatabaseCommand.CommandText = $@"SELECT * FROM {tableName} WHERE ROWNUM <= 1";
            DatabaseCommand.CommandType = CommandType.Text;
            return await GetAllColumnTableAsync(tableName, DatabaseCommand);
        }

        public override async Task<DataTable> GetDataTableAsync(string queryString, List<CDbQueryParamBind> bindParam = null) {
            DatabaseCommand.CommandText = queryString;
            DatabaseCommand.CommandType = CommandType.Text;
            BindQueryParameter(bindParam);
            return await GetDataTableAsync(DatabaseCommand);
        }

        public override async Task<T> ExecScalarAsync<T>(string queryString, List<CDbQueryParamBind> bindParam = null) {
            DatabaseCommand.CommandText = queryString;
            DatabaseCommand.CommandType = CommandType.Text;
            BindQueryParameter(bindParam);
            return await ExecScalarAsync<T>(DatabaseCommand);
        }

        public override async Task<bool> ExecQueryAsync(string queryString, List<CDbQueryParamBind> bindParam = null) {
            DatabaseCommand.CommandText = queryString;
            DatabaseCommand.CommandType = CommandType.Text;
            BindQueryParameter(bindParam);
            return await ExecQueryAsync(DatabaseCommand);
        }

        public override async Task<CDbExecProcResult> ExecProcedureAsync(string procedureName, List<CDbQueryParamBind> bindParam = null) {
            DatabaseCommand.CommandText = procedureName;
            DatabaseCommand.CommandType = CommandType.StoredProcedure;
            BindQueryParameter(bindParam);
            return await ExecProcedureAsync(DatabaseCommand);
        }

        public override async Task<int> UpdateTable(DataSet dataSet, string dataSetTableName, string queryString, List<CDbQueryParamBind> bindParam = null) {
            DatabaseCommand.CommandText = queryString;
            DatabaseCommand.CommandType = CommandType.Text;
            BindQueryParameter(bindParam);
            return await UpdateTable(DatabaseAdapter, dataSet, dataSetTableName);
        }

        public override async Task<bool> BulkInsertInto(string tableName, DataTable dataTable) {
            bool result = false;
            Exception exception = null;
            OracleBulkCopy dbBulkCopy = null;
            try {
                await OpenConnection();
                dbBulkCopy = new OracleBulkCopy((OracleConnection) DatabaseConnection) {
                    DestinationTableName = tableName
                };
                dbBulkCopy.WriteToServer(dataTable);
                result = true;
            }
            catch (Exception ex) {
                _logger.WriteError(ex, 4);
                exception = ex;
            }
            finally {
                if (dbBulkCopy != null) {
                    dbBulkCopy.Close();
                }
                CloseConnection();
            }
            return (exception == null) ? result : throw exception;
        }

        /// <summary> Jangan Lupa Di Close Koneksinya (Wajib) </summary>
        public override async Task<DbDataReader> ExecReaderAsync(string queryString, List<CDbQueryParamBind> bindParam = null) {
            DatabaseCommand.CommandText = queryString;
            DatabaseCommand.CommandType = CommandType.Text;
            BindQueryParameter(bindParam);
            return await ExecReaderAsync(DatabaseCommand);
        }

        public override async Task<string> RetrieveBlob(string stringPathDownload, string stringFileName, string queryString, List<CDbQueryParamBind> bindParam = null) {
            DatabaseCommand.CommandText = queryString;
            DatabaseCommand.CommandType = CommandType.Text;
            BindQueryParameter(bindParam);
            return await RetrieveBlob(DatabaseCommand, stringPathDownload, stringFileName);
        }

        public COracle NewExternalConnection(string dbIpAddrss, string dbPort, string dbUsername, string dbPassword, string dbNameSid) {
            COracle oracle = (COracle) Clone();
            string dbTnsOdp = $"(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST={dbIpAddrss})(PORT={dbPort})))(CONNECT_DATA=(SERVICE_NAME={dbNameSid})))";
            oracle.InitializeOracleDatabase(dbUsername, dbPassword, dbTnsOdp);
            oracle.SettingUpDatabase();
            return oracle;
        }

    }

}
