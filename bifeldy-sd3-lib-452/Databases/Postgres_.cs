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
 *              :: Instance Postgre
 * 
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

using Npgsql;

using bifeldy_sd3_lib_452.Abstractions;
using bifeldy_sd3_lib_452.Models;
using bifeldy_sd3_lib_452.Utilities;

namespace bifeldy_sd3_lib_452.Databases {

    public interface IPostgres : IDatabase { }

    public sealed class CPostgres : CDatabase, IPostgres {

        private readonly IApplication _app;
        private readonly ILogger _logger;

        private NpgsqlCommand DatabaseCommand { get; set; }
        private NpgsqlDataAdapter DatabaseAdapter { get; set; }

        public CPostgres(IApplication app, ILogger logger) : base(logger) {
            _app = app;
            _logger = logger;

            InitializePostgresDatabase();
        }

        private void InitializePostgresDatabase() {
            DbIpAddrss = _app.GetVariabel("IPPostgres");
            DbPort = _app.GetVariabel("PortPostgres");
            DbUsername = _app.GetVariabel("UserPostgres");
            DbPassword = _app.GetVariabel("PasswordPostgres");
            DbName = _app.GetVariabel("DatabasePostgres");
            try {
                DbConnectionString = $"Host={DbIpAddrss};Port={DbPort};Username={DbUsername};Password={DbPassword};Database={DbName};";
                DatabaseConnection = new NpgsqlConnection(DbConnectionString);
                DatabaseCommand = new NpgsqlCommand("", (NpgsqlConnection) DatabaseConnection);
                DatabaseAdapter = new NpgsqlDataAdapter(DatabaseCommand);
                _logger.WriteInfo(GetType().Name, DbConnectionString);
            }
            catch {
                //
            }
        }

        /** Bagian Ini Mirip :: Oracle - Ms. Sql Server - PostgreSQL */

        private void BindQueryParameter(List<CDbQueryParamBind> parameters) {
            DatabaseCommand.Parameters.Clear();
            if (parameters != null) {
                for (int i = 0; i < parameters.Count; i++) {
                    dynamic pVal = parameters[i].VALUE;
                    Type pValType = (pVal == null) ? typeof(DBNull) : pVal.GetType();
                    if (pValType.IsArray) {
                        string bindStr = "";
                        int id = 1;
                        foreach (dynamic data in pVal) {
                            if (!string.IsNullOrEmpty(bindStr)) {
                                bindStr += ", ";
                            }
                            bindStr += $":{parameters[i].NAME}_{id}";
                            DatabaseCommand.Parameters.Add(new NpgsqlParameter {
                                ParameterName = $"{parameters[i].NAME}_{id}",
                                Value = data ?? DBNull.Value
                            });
                            id++;
                        }
                        Regex regex = new Regex($":{parameters[i].NAME}");
                        DatabaseCommand.CommandText = regex.Replace(DatabaseCommand.CommandText, bindStr, 1);
                    }
                    else {
                        NpgsqlParameter param = new NpgsqlParameter {
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

        public override async Task<DataTable> GetDataTableAsync(string queryString, List<CDbQueryParamBind> bindParam = null) {
            DatabaseCommand.CommandText = queryString;
            DatabaseCommand.CommandType = CommandType.Text;
            BindQueryParameter(bindParam);
            return await GetDataTableAsync(DatabaseAdapter);
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
            string sqlTextQueryParameters = "(";
            for (int i = 0; i < bindParam.Count; i++) {
                sqlTextQueryParameters += $":{bindParam[i].NAME}";
                if (i + 1 < bindParam.Count) sqlTextQueryParameters += ",";
            }
            sqlTextQueryParameters += ")";
            DatabaseCommand.CommandText = $"CALL {procedureName} {sqlTextQueryParameters}";
            DatabaseCommand.CommandType = CommandType.Text;
            BindQueryParameter(bindParam);
            return await ExecProcedureAsync(DatabaseCommand);
        }

        public override async Task<int> UpdateTable(DataSet dataSet, string dataSetTableName, string queryString, List<CDbQueryParamBind> bindParam = null) {
            DatabaseCommand.CommandText = queryString;
            DatabaseCommand.CommandType = CommandType.Text;
            BindQueryParameter(bindParam);
            return await UpdateTable(DatabaseAdapter, dataSet, dataSetTableName);
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

    }

}
