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
 *              :: Instance Microsoft SQL Server
 * 
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

using bifeldy_sd3_lib_452.Abstractions;
using bifeldy_sd3_lib_452.Models;
using bifeldy_sd3_lib_452.Utilities;

namespace bifeldy_sd3_lib_452.Databases {

    public interface IMsSQL : IDatabase {
        IMsSQL NewExternalConnection(string dbIpAddrss, string dbUsername, string dbPassword, string dbName);
    }

    public sealed class CMsSQL : CDatabase, IMsSQL {

        private SqlCommand DatabaseCommand { get; set; }
        private SqlDataAdapter DatabaseAdapter { get; set; }

        public CMsSQL(IApplication app, ILogger logger, IConverter converter, ICsv csv, ILocker locker) : base(app, logger, converter, csv, locker) {
            this.InitializeMsSqlDatabase();
            this.SettingUpDatabase();
        }

        private void InitializeMsSqlDatabase(string dbIpAddrss = null, string dbUsername = null, string dbPassword = null, string dbName = null) {
            this.DbIpAddrss = dbIpAddrss ?? this._app.GetVariabel("IPSql");
            this.DbUsername = dbUsername ?? this._app.GetVariabel("UserSql");
            this.DbPassword = dbPassword ?? this._app.GetVariabel("PasswordSql");
            this.DbName = dbName ?? this._app.GetVariabel("DatabaseSql");
        }

        private void SettingUpDatabase() {
            try {
                this.DbConnectionString = $"Data Source={this.DbIpAddrss};Initial Catalog={this.DbName};User ID={this.DbUsername};Password={this.DbPassword};Connection Timeout=180;"; // 3 Minutes
                if (
                    string.IsNullOrEmpty(this.DbIpAddrss) ||
                    string.IsNullOrEmpty(this.DbName) ||
                    string.IsNullOrEmpty(this.DbUsername) ||
                    string.IsNullOrEmpty(this.DbPassword)
                ) {
                    throw new Exception("Database MsSQL Tidak Tersedia");
                }

                this.DatabaseConnection = new SqlConnection(this.DbConnectionString);
                if (this._app.DebugMode) {
                    ((SqlConnection) this.DatabaseConnection).InfoMessage += (_, evt) => {
                        this._logger.WriteInfo(this.GetType().Name, evt.Message);
                    };
                }

                this.DatabaseCommand = new SqlCommand {
                    Connection = (SqlConnection) this.DatabaseConnection,
                    CommandTimeout = 3600 // 60 Minutes
                };
                this.DatabaseAdapter = new SqlDataAdapter(this.DatabaseCommand);
                this._logger.WriteInfo(this.GetType().Name, this.DbConnectionString);
            }
            catch (Exception ex) {
                this._logger.WriteError(ex);
            }
        }

        protected override void BindQueryParameter(List<CDbQueryParamBind> parameters) {
            char prefix = '@';
            this.DatabaseCommand.Parameters.Clear();
            if (parameters != null) {
                for (int i = 0; i < parameters.Count; i++) {
                    string pName = parameters[i].NAME.StartsWith($"{prefix}") ? parameters[i].NAME.Substring(1) : parameters[i].NAME;
                    if (string.IsNullOrEmpty(pName)) {
                        throw new Exception("Nama Parameter Wajib Diisi");
                    }

                    dynamic pVal = parameters[i].VALUE;
                    Type pValType = (pVal == null) ? typeof(DBNull) : pVal.GetType();

                    if (pValType.IsArray || (pValType.IsGenericType && pValType.GetGenericTypeDefinition() == typeof(List<>))) {
                        string bindStr = string.Empty;
                        int id = 1;
                        foreach (dynamic data in pVal) {
                            if (!string.IsNullOrEmpty(bindStr)) {
                                bindStr += ", ";
                            }

                            bindStr += $"{prefix}{pName}_{id}";

                            var param = new SqlParameter {
                                ParameterName = $"{pName}_{id}",
                                Value = data ?? DBNull.Value
                            };

                            if (parameters[i].SIZE > 0) {
                                param.Size = parameters[i].SIZE;
                            }

                            if (parameters[i].DIRECTION > 0) {
                                param.Direction = parameters[i].DIRECTION;
                            }

                            if (parameters[i].DATA_TYPE != null) {
                                if (parameters[i].DATA_TYPE.GetType() == typeof(SqlDbType)) {
                                    param.SqlDbType = parameters[i].DATA_TYPE;
                                }
                                else {
                                    param.SqlDbType = Enum.Parse(typeof(SqlDbType), parameters[i].DATA_TYPE, true);
                                }
                            }

                            _ = this.DatabaseCommand.Parameters.Add(param);

                            id++;
                        }

                        var regex = new Regex($"{prefix}{pName}");
                        this.DatabaseCommand.CommandText = regex.Replace(this.DatabaseCommand.CommandText, bindStr, 1);
                    }
                    else {
                        var param = new SqlParameter {
                            ParameterName = pName,
                            Value = pVal ?? DBNull.Value
                        };

                        if (parameters[i].SIZE > 0) {
                            param.Size = parameters[i].SIZE;
                        }

                        if (parameters[i].DIRECTION > 0) {
                            param.Direction = parameters[i].DIRECTION;
                        }

                        if (parameters[i].DATA_TYPE != null) {
                            if (parameters[i].DATA_TYPE.GetType() == typeof(SqlDbType)) {
                                param.SqlDbType = parameters[i].DATA_TYPE;
                            }
                            else {
                                param.SqlDbType = Enum.Parse(typeof(SqlDbType), parameters[i].DATA_TYPE, true);
                            }
                        }

                        _ = this.DatabaseCommand.Parameters.Add(param);
                    }
                }
            }

            this.LogQueryParameter(this.DatabaseCommand, prefix);
        }

        public override async Task<DataColumnCollection> GetAllColumnTableAsync(string tableName) {
            this.DatabaseCommand.CommandText = $@"SELECT * FROM {tableName} LIMIT 1";
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

        public override async Task<int> ExecQueryWithResultAsync(string queryString, List<CDbQueryParamBind> bindParam = null) {
            this.DatabaseCommand.CommandText = queryString;
            this.DatabaseCommand.CommandType = CommandType.Text;
            this.BindQueryParameter(bindParam);
            return await this.ExecQueryWithResultAsync(this.DatabaseCommand);
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
                _ = await this._locker.MutexGlobalApp.WaitAsync(-1);
                await this.OpenConnection();
                using (var dbBulkCopy = new SqlBulkCopy((SqlConnection) this.DatabaseConnection) {
                    DestinationTableName = tableName
                }) {
                    await dbBulkCopy.WriteToServerAsync(dataTable);
                    result = true;
                }
            }
            catch (Exception ex) {
                this._logger.WriteError(ex, 4);
                exception = ex;
            }
            finally {
                this.CloseConnection();
                _ = this._locker.MutexGlobalApp.Release();
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

        public IMsSQL NewExternalConnection(string dbIpAddrss, string dbUsername, string dbPassword, string dbName) {
            var mssql = (CMsSQL) this.Clone();
            mssql.InitializeMsSqlDatabase(dbIpAddrss, dbUsername, dbPassword, dbName);
            mssql.SettingUpDatabase();
            return mssql;
        }

        public override IDatabase CloneConnection() {
            return this.NewExternalConnection(this.DbIpAddrss, this.DbUsername, this.DbPassword, this.DbName);
        }

    }

}
