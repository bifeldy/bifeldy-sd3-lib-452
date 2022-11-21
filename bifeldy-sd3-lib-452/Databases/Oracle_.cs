/**
 * 
 * Author       :: Basilius Bias Astho Christyono
 * Mail         :: bias@indomaret.co.id
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

    public interface IOracle : IDatabase { }

    public sealed class COracle : CDatabase, IOracle {

        private readonly IApp _app;
        private readonly ILogger _logger;

        private OracleCommand DatabaseCommand { get; set; }
        private OracleDataAdapter DatabaseAdapter { get; set; }

        public COracle(IApp app, ILogger logger) : base(logger) {
            _app = app;
            _logger = logger;

            InitializeOracleDatabase();
        }

        private void InitializeOracleDatabase() {
            DbUsername = _app.GetVariabelOraSql("UserOrcl");
            DbPassword = _app.GetVariabelOraSql("PasswordOrcl");
            DbTnsOdp = _app.GetVariabelOraSql("ODPOrcl");
            DbName = DbTnsOdp.Split(
                new string[] { "SERVICE_NAME=" },
                StringSplitOptions.None
            )[1].Split(
                new string[] { ")" },
                StringSplitOptions.None
            )[0];
            try {
                DbConnectionString = $"Data Source={DbTnsOdp};User ID={DbUsername};Password={DbPassword};";
                DatabaseConnection = new OracleConnection(DbConnectionString);
                DatabaseCommand = new OracleCommand("", (OracleConnection)DatabaseConnection);
                DatabaseCommand.BindByName = true;
                DatabaseAdapter = new OracleDataAdapter(DatabaseCommand);
                _logger.WriteLog(GetType().Name, DbConnectionString);
            }
            catch {
                //
            }
        }

        private void BindQueryParameter(List<CDbQueryParamBind> parameters) {
            DatabaseCommand.Parameters.Clear();
            if (parameters != null) {
                for (int i = 0; i < parameters.Count; i++) {
                    if (parameters[i].VALUE.GetType().IsArray) {
                        string bindStr = "";
                        int id = 1;
                        foreach (dynamic data in parameters[i].VALUE) {
                            if (!string.IsNullOrEmpty(bindStr)) {
                                bindStr += ", ";
                            }
                            bindStr += $":{parameters[i].NAME}_{id}";
                            DatabaseCommand.Parameters.Add(new OracleParameter {
                                ParameterName = $"{parameters[i].NAME}_{id}",
                                Value = data
                            });
                            id++;
                        }
                        DatabaseCommand.CommandText = DatabaseCommand.CommandText.Replace($":{parameters[i].NAME}", bindStr);
                    }
                    else {
                        OracleParameter param = new OracleParameter {
                            ParameterName = parameters[i].NAME,
                            Value = parameters[i].VALUE
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
            string sqlTextQueryParameters = DatabaseCommand.CommandText;
            for (int i = 0; i < DatabaseCommand.Parameters.Count; i++) {
                object val = DatabaseCommand.Parameters[i].Value;
                if (DatabaseCommand.Parameters[i].Value.GetType() == typeof(string)) {
                    val = $"'{DatabaseCommand.Parameters[i].Value}'";
                }
                sqlTextQueryParameters = sqlTextQueryParameters.Replace($":{DatabaseCommand.Parameters[i].ParameterName}", val.ToString());
            }
            sqlTextQueryParameters = sqlTextQueryParameters.Replace($"\r\n", " ");
            sqlTextQueryParameters = Regex.Replace(sqlTextQueryParameters, @"\s+", " ");
            _logger.WriteLog(GetType().Name, sqlTextQueryParameters.Trim());
        }

        /** Bagian Ini Mirip :: Oracle - Ms. Sql Server - PostgreSQL */

        public override async Task<(DataTable, Exception)> GetDataTableAsync(string queryString, List<CDbQueryParamBind> bindParam = null, bool closeConnection = true) {
            DatabaseCommand.CommandText = queryString;
            DatabaseCommand.CommandType = CommandType.Text;
            BindQueryParameter(bindParam);
            return await GetDataTableAsync(DatabaseAdapter, closeConnection);
        }

        public override async Task<(T, Exception)> ExecScalarAsync<T>(string queryString, List<CDbQueryParamBind> bindParam = null, bool closeConnection = true) {
            DatabaseCommand.CommandText = queryString;
            DatabaseCommand.CommandType = CommandType.Text;
            BindQueryParameter(bindParam);
            return await ExecScalarAsync<T>(DatabaseCommand, closeConnection);
        }

        public override async Task<(bool, Exception)> ExecQueryAsync(string queryString, List<CDbQueryParamBind> bindParam = null, bool closeConnection = true) {
            DatabaseCommand.CommandText = queryString;
            DatabaseCommand.CommandType = CommandType.Text;
            BindQueryParameter(bindParam);
            return await ExecQueryAsync(DatabaseCommand, closeConnection);
        }

        public override async Task<(CDbExecProcResult, Exception)> ExecProcedureAsync(string procedureName, List<CDbQueryParamBind> bindParam = null, bool closeConnection = true) {
            DatabaseCommand.CommandText = procedureName;
            DatabaseCommand.CommandType = CommandType.StoredProcedure;
            BindQueryParameter(bindParam);
            return await ExecProcedureAsync(DatabaseCommand, closeConnection);
        }

        public override async Task<(int, Exception)> UpdateTable(DataSet dataSet, string dataSetTableName, string queryString, List<CDbQueryParamBind> bindParam = null, bool closeConnection = true) {
            DatabaseCommand.CommandText = queryString;
            DatabaseCommand.CommandType = CommandType.Text;
            BindQueryParameter(bindParam);
            return await UpdateTable(DatabaseAdapter, dataSet, dataSetTableName, closeConnection);
        }

        /// <summary>Jangan Lupa Di Close !!</summary>
        public override async Task<(DbDataReader, Exception)> ExecReaderAsync(string queryString, List<CDbQueryParamBind> bindParam = null, bool closeConnection = false) {
            DatabaseCommand.CommandText = queryString;
            DatabaseCommand.CommandType = CommandType.Text;
            BindQueryParameter(bindParam);
            return await ExecReaderAsync(DatabaseCommand, closeConnection);
        }

        /// <summary>Jangan Lupa Di Close !!</summary>
        public override async Task<(string, Exception)> RetrieveBlob(string stringPathDownload, string stringFileName, string queryString, List<CDbQueryParamBind> bindParam = null, bool closeConnection = false) {
            DatabaseCommand.CommandText = queryString;
            DatabaseCommand.CommandType = CommandType.Text;
            BindQueryParameter(bindParam);
            return await RetrieveBlob(DatabaseCommand, stringPathDownload, stringFileName, closeConnection);
        }

    }

}
