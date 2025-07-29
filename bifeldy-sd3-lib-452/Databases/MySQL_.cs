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
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

using MySql.Data.MySqlClient;

using bifeldy_sd3_lib_452.Abstractions;
using bifeldy_sd3_lib_452.Models;
using bifeldy_sd3_lib_452.Utilities;

namespace bifeldy_sd3_lib_452.Databases {

    public interface IMySQL : IDatabase {
        IMySQL NewExternalConnection(string dbIpAddrss, string dbUsername, string dbPassword, string dbName);
    }

    public sealed class CMySQL : CDatabase, IMySQL {

        private MySqlCommand DatabaseCommand { get; set; }
        private MySqlDataAdapter DatabaseAdapter { get; set; }

        public CMySQL(IApplication app, ILogger logger, IConverter converter, ICsv csv, ILocker locker) : base(app, logger, converter, csv, locker) {
            this.InitializeMySqlDatabase();
            this.SettingUpDatabase();
        }

        private void InitializeMySqlDatabase(string dbIpAddrss = null, string dbUsername = null, string dbPassword = null, string dbName = null) {
            this.DbIpAddrss = dbIpAddrss ?? this._app.GetVariabel("mysqlip") ?? this._app.GetVariabel("MysqlServer");
            this.DbUsername = dbUsername ?? this._app.GetVariabel("mysqluser") ?? this._app.GetVariabel("MysqlUid");
            this.DbPassword = dbPassword ?? this._app.GetVariabel("mysqlpass") ?? this._app.GetVariabel("MysqlPwd");
            this.DbName = dbName ?? this._app.GetVariabel("mysqldb") ?? this._app.GetVariabel("MysqlDatabase");
        }

        private void SettingUpDatabase() {
            try {
                this.DbConnectionString = $"Server={this.DbIpAddrss};Database={this.DbName};Uid={this.DbUsername};Password={this.DbPassword};Connection Timeout=180;"; // 3 Minutes
                if (
                    string.IsNullOrEmpty(this.DbIpAddrss) ||
                    string.IsNullOrEmpty(this.DbName) ||
                    string.IsNullOrEmpty(this.DbUsername) ||
                    string.IsNullOrEmpty(this.DbPassword)
                ) {
                    throw new Exception("Database Tidak Tersedia");
                }

                this.DatabaseConnection = new MySqlConnection(this.DbConnectionString);
                this.DatabaseCommand = new MySqlCommand {
                    Connection = (MySqlConnection) this.DatabaseConnection,
                    CommandTimeout = 3600 // 60 Minutes
                };
                this.DatabaseAdapter = new MySqlDataAdapter(this.DatabaseCommand);
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
                    if (pValType.IsArray) {
                        string bindStr = string.Empty;
                        int id = 1;
                        foreach (dynamic data in pVal) {
                            if (!string.IsNullOrEmpty(bindStr)) {
                                bindStr += ", ";
                            }

                            bindStr += $"{prefix}{pName}_{id}";
                            _ = this.DatabaseCommand.Parameters.Add(new MySqlParameter {
                                ParameterName = $"{pName}_{id}",
                                Value = data ?? DBNull.Value
                            });
                            id++;
                        }

                        var regex = new Regex($"{prefix}{pName}");
                        this.DatabaseCommand.CommandText = regex.Replace(this.DatabaseCommand.CommandText, bindStr, 1);
                    }
                    else {
                        var param = new MySqlParameter {
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

                if (string.IsNullOrEmpty(tableName)) {
                    throw new Exception("Target Tabel Tidak Ditemukan");
                }

                int colCount = dataTable.Columns.Count;

                var types = new Type[colCount];
                int[] lengths = new int[colCount];
                string[] fieldNames = new string[colCount];

                this.DatabaseCommand.CommandText = $"SELECT * FROM {tableName} WHERE 1 = 0";
                using (var rdr = (MySqlDataReader) await this.ExecReaderAsync(this.DatabaseCommand)) {
                    if (rdr.FieldCount != colCount) {
                        throw new Exception("Jumlah Kolom Tabel Tidak Sama");
                    }

                    DataColumnCollection columns = rdr.GetSchemaTable().Columns;
                    for (int i = 0; i < colCount; i++) {
                        types[i] = columns[i].DataType;
                        lengths[i] = columns[i].MaxLength;
                        fieldNames[i] = columns[i].ColumnName;
                    }
                }

                var param = new List<CDbQueryParamBind>();
                var sB = new StringBuilder($"INSERT INTO {tableName} (");

                string sbHeader = string.Empty;
                for (int c = 0; c < colCount; c++) {
                    if (!string.IsNullOrEmpty(sbHeader)) {
                        sbHeader += ", ";
                    }

                    sbHeader += fieldNames[c];
                }

                _ = sB.Append(sbHeader + ") VALUES (");
                string sbRow = "(";
                for (int r = 0; r < dataTable.Rows.Count; r++) {
                    if (sbRow.EndsWith(")")) {
                        sbRow += ", (";
                    }

                    string sbColumn = string.Empty;
                    for (int c = 0; c < colCount; c++) {
                        if (!string.IsNullOrEmpty(sbColumn)) {
                            sbColumn += ", ";
                        }

                        string paramKey = $"{fieldNames[c]}_{r}";
                        sbColumn += paramKey;
                        param.Add(new CDbQueryParamBind {
                            NAME = paramKey,
                            VALUE = dataTable.Rows[r][fieldNames[c]]
                        });
                    }

                    sbRow += $"{sbColumn} )";
                }

                _ = sB.Append(sbRow);

                string query = sB.ToString();
                await this.MarkBeforeCommitRollback();
                bool run = await this.ExecQueryAsync(query, param);
                this.MarkSuccessCommitAndClose();

                result = run;
            }
            catch (Exception ex) {
                this.MarkFailedRollbackAndClose();
                this._logger.WriteError(ex, 3);
                exception = ex;
            }
            finally {
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

        public IMySQL NewExternalConnection(string dbIpAddrss, string dbUsername, string dbPassword, string dbName) {
            var mssql = (CMySQL) this.Clone();
            mssql.InitializeMySqlDatabase(dbIpAddrss, dbUsername, dbPassword, dbName);
            mssql.SettingUpDatabase();
            return mssql;
        }

        public override IDatabase CloneConnection() {
            return this.NewExternalConnection(this.DbIpAddrss, this.DbUsername, this.DbPassword, this.DbName);
        }

    }

}
