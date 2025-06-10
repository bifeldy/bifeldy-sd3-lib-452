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
using System.Text;

namespace bifeldy_sd3_lib_452.Databases {

    public interface IOracle : IDatabase {
        COracle NewExternalConnection(string dbIpAddrss, string dbPort, string dbUsername, string dbPassword, string dbNameSid);
        COracle CloneConnection();
    }

    public sealed class COracle : CDatabase, IOracle {

        private readonly IApplication _app;
        private readonly ILogger _logger;

        private OracleCommand DatabaseCommand { get; set; }
        private OracleDataAdapter DatabaseAdapter { get; set; }

        public COracle(IApplication app, ILogger logger, IConverter converter, ICsv csv) : base(logger, converter, csv) {
            this._app = app;
            this._logger = logger;

            this.InitializeOracleDatabase();
            this.SettingUpDatabase();
        }

        private void InitializeOracleDatabase(string dbUsername = null, string dbPassword = null, string dbTnsOdp = null) {
            this.DbUsername = dbUsername ?? this._app.GetVariabel("UserOrcl");
            this.DbPassword = dbPassword ?? this._app.GetVariabel("PasswordOrcl");
            string _dbTnsOdp = dbTnsOdp ?? this._app.GetVariabel("ODPOrcl");
            if (!string.IsNullOrEmpty(_dbTnsOdp)) {
                _dbTnsOdp = Regex.Replace(_dbTnsOdp, @"\s+", "");
            }

            this.DbTnsOdp = _dbTnsOdp;
        }

        private void SettingUpDatabase() {
            try {
                string _dbName = null;
                if (!string.IsNullOrEmpty(this.DbTnsOdp)) {
                    _dbName = this.DbTnsOdp.Split(new string[] { "SERVICE_NAME=" }, StringSplitOptions.None)[1].Split(new string[] { ")" }, StringSplitOptions.None)[0];
                }

                this.DbName = _dbName;
                this.DbConnectionString = $"Data Source={this.DbTnsOdp};User ID={this.DbUsername};Password={this.DbPassword};Connection Timeout=180;"; // 3 Minutes
                if (
                    string.IsNullOrEmpty(this.DbTnsOdp) ||
                    string.IsNullOrEmpty(this.DbUsername) ||
                    string.IsNullOrEmpty(this.DbPassword)
                ) {
                    throw new Exception("Database Tidak Tersedia");
                }

                this.DatabaseConnection = new OracleConnection(this.DbConnectionString);
                if (this._app.DebugMode) {
                    ((OracleConnection) this.DatabaseConnection).InfoMessage += (_, evt) => {
                        this._logger.WriteInfo(this.GetType().Name, evt.Message);
                    };
                }

                this.DatabaseCommand = new OracleCommand {
                    Connection = (OracleConnection) this.DatabaseConnection,
                    BindByName = true,
                    InitialLOBFetchSize = -1,
                    InitialLONGFetchSize = -1,
                    CommandTimeout = 3600 // 60 Minutes
                };
                this.DatabaseAdapter = new OracleDataAdapter(this.DatabaseCommand);
                this._logger.WriteInfo(this.GetType().Name, this.DbConnectionString);
            }
            catch (Exception ex) {
                this._logger.WriteError(ex);
            }
        }

        protected override void BindQueryParameter(List<CDbQueryParamBind> parameters) {
            char prefix = ':';
            this.DatabaseCommand.Parameters.Clear();
            if (parameters != null) {
                for (int i = 0; i < parameters.Count; i++) {
                    string pName = parameters[i].NAME.StartsWith($"{prefix}") ? parameters[i].NAME.Substring(1) : parameters[i].NAME;
                    if (string.IsNullOrEmpty(pName)) {
                        throw new Exception("Nama Parameter Wajib Diisi");
                    }

                    dynamic pVal = parameters[i].VALUE;
                    Type pValType = (pVal == null) ? typeof(DBNull) : pVal.GetType();
                    if (pValType.IsArray) {
                        string bindStr = string.Empty;
                        int id = 1;
                        foreach (dynamic data in pVal) {
                            if (!string.IsNullOrEmpty(bindStr)) {
                                bindStr += ", ";
                            }

                            bindStr += $"{prefix}{pName}_{id}";
                            _ = this.DatabaseCommand.Parameters.Add(new OracleParameter {
                                ParameterName = $"{pName}_{id}",
                                Value = data ?? DBNull.Value
                            });
                            id++;
                        }

                        var regex = new Regex($"{prefix}{pName}");
                        this.DatabaseCommand.CommandText = regex.Replace(this.DatabaseCommand.CommandText, bindStr, 1);
                    }
                    else {
                        var param = new OracleParameter {
                            ParameterName = pName,
                            Value = pVal ?? DBNull.Value
                        };
                        if (parameters[i].SIZE > 0) {
                            param.Size = parameters[i].SIZE;
                        }

                        if (parameters[i].DIRECTION > 0) {
                            param.Direction = parameters[i].DIRECTION;
                        }

                        _ = this.DatabaseCommand.Parameters.Add(param);
                    }
                }
            }

            this.LogQueryParameter(this.DatabaseCommand, prefix);
        }

        public override async Task<DataColumnCollection> GetAllColumnTableAsync(string tableName) {
            this.DatabaseCommand.CommandText = $@"SELECT * FROM {tableName} WHERE ROWNUM <= 1";
            this.DatabaseCommand.CommandType = CommandType.Text;
            return await this.GetAllColumnTableAsync(tableName, this.DatabaseCommand);
        }

        public override async Task<DataTable> GetDataTableAsync(string queryString, List<CDbQueryParamBind> bindParam = null) {
            this.DatabaseCommand.CommandText = queryString;
            this.DatabaseCommand.CommandType = CommandType.Text;
            this.BindQueryParameter(bindParam);
            return await this.GetDataTableAsync(this.DatabaseCommand);
        }

        public override async Task<List<T>> GetListAsync<T>(string queryString, List<CDbQueryParamBind> bindParam = null) {
            this.DatabaseCommand.CommandText = queryString;
            this.DatabaseCommand.CommandType = CommandType.Text;
            this.BindQueryParameter(bindParam);
            return await this.GetListAsync<T>(this.DatabaseCommand);
        }

        public override async Task<T> ExecScalarAsync<T>(string queryString, List<CDbQueryParamBind> bindParam = null) {
            this.DatabaseCommand.CommandText = queryString;
            this.DatabaseCommand.CommandType = CommandType.Text;
            this.BindQueryParameter(bindParam);
            return await this.ExecScalarAsync<T>(this.DatabaseCommand);
        }

        public override async Task<bool> ExecQueryAsync(string queryString, List<CDbQueryParamBind> bindParam = null, int minRowsAffected = 1, bool shouldEqualMinRowsAffected = false) {
            this.DatabaseCommand.CommandText = queryString;
            this.DatabaseCommand.CommandType = CommandType.Text;
            this.BindQueryParameter(bindParam);
            return await this.ExecQueryAsync(this.DatabaseCommand, minRowsAffected, shouldEqualMinRowsAffected);
        }

        public override async Task<CDbExecProcResult> ExecProcedureAsync(string procedureName, List<CDbQueryParamBind> bindParam = null) {
            this.DatabaseCommand.CommandText = procedureName;
            this.DatabaseCommand.CommandType = CommandType.StoredProcedure;
            this.BindQueryParameter(bindParam);
            return await this.ExecProcedureAsync(this.DatabaseCommand);
        }

        public override async Task<bool> BulkInsertInto(string tableName, DataTable dataTable) {
            bool result = false;
            Exception exception = null;
            try {
                await this.OpenConnection();
                using (var dbBulkCopy = new OracleBulkCopy((OracleConnection) this.DatabaseConnection) {
                    DestinationTableName = tableName
                }) {
                    dbBulkCopy.WriteToServer(dataTable);
                    result = true;
                }
            }
            catch (Exception ex) {
                this._logger.WriteError(ex, 4);
                exception = ex;
            }
            finally {
                this.CloseConnection();
            }

            return (exception == null) ? result : throw exception;
        }

        /// <summary> Jangan Lupa Di Close Koneksinya (Wajib) </summary>
        public override async Task<DbDataReader> ExecReaderAsync(string queryString, List<CDbQueryParamBind> bindParam = null, CommandBehavior commandBehavior = CommandBehavior.Default) {
            this.DatabaseCommand.CommandText = queryString;
            this.DatabaseCommand.CommandType = CommandType.Text;
            this.BindQueryParameter(bindParam);
            return await this.ExecReaderAsync(this.DatabaseCommand, commandBehavior);
        }

        public override async Task<List<string>> RetrieveBlob(string stringPathDownload, string queryString, List<CDbQueryParamBind> bindParam = null, string stringCustomSingleFileName = null, Encoding encoding = null) {
            this.DatabaseCommand.CommandText = queryString;
            this.DatabaseCommand.CommandType = CommandType.Text;
            this.BindQueryParameter(bindParam);
            return await this.RetrieveBlob(this.DatabaseCommand, stringPathDownload, stringCustomSingleFileName, encoding ?? Encoding.UTF8);
        }

        public COracle NewExternalConnection(string dbIpAddrss, string dbPort, string dbUsername, string dbPassword, string dbNameSid) {
            var oracle = (COracle) this.Clone();
            string dbTnsOdp = $"(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST={dbIpAddrss})(PORT={dbPort})))(CONNECT_DATA=(SERVICE_NAME={dbNameSid})))";
            oracle.InitializeOracleDatabase(dbUsername, dbPassword, dbTnsOdp);
            oracle.SettingUpDatabase();
            return oracle;
        }

        public COracle CloneConnection() {
            var oracle = (COracle) this.Clone();
            oracle.InitializeOracleDatabase(this.DbUsername, this.DbPassword, this.DbTnsOdp);
            oracle.SettingUpDatabase();
            return oracle;
        }

    }

}
