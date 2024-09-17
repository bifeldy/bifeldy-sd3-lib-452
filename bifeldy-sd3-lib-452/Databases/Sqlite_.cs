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
 *              :: Instance Postgre
 * 
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SQLite;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

using bifeldy_sd3_lib_452.Abstractions;
using bifeldy_sd3_lib_452.Models;
using bifeldy_sd3_lib_452.Utilities;

namespace bifeldy_sd3_lib_452.Databases {

    public interface ISqlite : IDatabase {
        CSqlite NewExternalConnection(string dbName);
    }

    public sealed class CSqlite : CDatabase, ISqlite {

        private readonly IApplication _app;
        private readonly IConfig _config;
        private readonly ILogger _logger;

        private SQLiteCommand DatabaseCommand { get; set; }
        private SQLiteDataAdapter DatabaseAdapter { get; set; }

        public CSqlite(IApplication app, IConfig config, ILogger logger, IConverter converter) : base(logger, converter) {
            this._app = app;
            this._config = config;
            this._logger = logger;

            this.InitializeSqliteDatabase();
            this.SettingUpDatabase();
        }

        private void InitializeSqliteDatabase(string dbName = null) {
            this.DbName = dbName ?? this._config.Get<string>("LocalDbName", this._app.GetConfig("local_db_name"));
        }

        private void SettingUpDatabase() {
            try {
                this.DbConnectionString = $"Data Source={this.DbName}";
                if (string.IsNullOrEmpty(this.DbName)) {
                    throw new Exception("Database Tidak Tersedia");
                }

                this.DatabaseConnection = new SQLiteConnection(this.DbConnectionString);
                this.DatabaseCommand = new SQLiteCommand {
                    Connection = (SQLiteConnection) this.DatabaseConnection,
                    CommandTimeout = 1800 // 30 menit
                };
                this.DatabaseAdapter = new SQLiteDataAdapter(this.DatabaseCommand);
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

                            bindStr += $"{pName}_{id}";
                            this.DatabaseCommand.Parameters.Add(new SQLiteParameter {
                                ParameterName = $"{prefix}{pName}_{id}",
                                Value = data ?? DBNull.Value
                            });
                            id++;
                        }

                        var regex = new Regex($"{prefix}{pName}");
                        this.DatabaseCommand.CommandText = regex.Replace(this.DatabaseCommand.CommandText, bindStr, 1);
                    }
                    else {
                        var param = new SQLiteParameter {
                            ParameterName = pName,
                            Value = pVal ?? DBNull.Value
                        };
                        if (parameters[i].SIZE > 0) {
                            param.Size = parameters[i].SIZE;
                        }

                        if (parameters[i].DIRECTION > 0) {
                            param.Direction = parameters[i].DIRECTION;
                        }

                        this.DatabaseCommand.Parameters.Add(param);
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

        public override async Task<T> ExecScalarAsync<T>(string queryString, List<CDbQueryParamBind> bindParam = null) {
            this.DatabaseCommand.CommandText = queryString;
            this.DatabaseCommand.CommandType = CommandType.Text;
            this.BindQueryParameter(bindParam);
            return await this.ExecScalarAsync<T>(this.DatabaseCommand);
        }

        public override async Task<bool> ExecQueryAsync(string queryString, List<CDbQueryParamBind> bindParam = null) {
            this.DatabaseCommand.CommandText = queryString;
            this.DatabaseCommand.CommandType = CommandType.Text;
            this.BindQueryParameter(bindParam);
            return await this.ExecQueryAsync(this.DatabaseCommand);
        }

        public override async Task<CDbExecProcResult> ExecProcedureAsync(string procedureName, List<CDbQueryParamBind> bindParam = null) {
            throw new Exception("SQLite Tidak Memiliki Stored Procedure");
            // string sqlTextQueryParameters = "(";
            // if (bindParam != null) {
            //     for (int i = 0; i < bindParam.Count; i++) {
            //         sqlTextQueryParameters += $":{bindParam[i].NAME}";
            //         if (i + 1 < bindParam.Count) sqlTextQueryParameters += ",";
            //     }
            // }
            // sqlTextQueryParameters += ")";
            // DatabaseCommand.CommandText = $"CALL {procedureName} {sqlTextQueryParameters}";
            // DatabaseCommand.CommandType = CommandType.StoredProcedure;
            // BindQueryParameter(bindParam);
            // return await ExecProcedureAsync(DatabaseCommand);
        }

        public override async Task<bool> BulkInsertInto(string tableName, DataTable dataTable) {
            bool result = false;
            Exception exception = null;
            try {
                if (string.IsNullOrEmpty(tableName)) {
                    throw new Exception("Target Tabel Tidak Ditemukan");
                }

                int colCount = dataTable.Columns.Count;

                var types = new Type[colCount];
                int[] lengths = new int[colCount];
                string[] fieldNames = new string[colCount];

                this.DatabaseCommand.CommandText = $"SELECT * FROM {tableName} WHERE 1 = 0";
                using (var rdr = (SQLiteDataReader) await this.ExecReaderAsync(this.DatabaseCommand)) {
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

                sB.Append(sbHeader + ") VALUES (");
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

                sB.Append(sbRow);

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

            return (exception == null) ? result : throw exception;
        }

        /// <summary> Jangan Lupa Di Close Koneksinya (Wajib) </summary>
        public override async Task<DbDataReader> ExecReaderAsync(string queryString, List<CDbQueryParamBind> bindParam = null) {
            this.DatabaseCommand.CommandText = queryString;
            this.DatabaseCommand.CommandType = CommandType.Text;
            this.BindQueryParameter(bindParam);
            return await this.ExecReaderAsync(this.DatabaseCommand);
        }

        public override async Task<List<string>> RetrieveBlob(string stringPathDownload, string queryString, List<CDbQueryParamBind> bindParam = null, string stringCustomSingleFileName = null) {
            this.DatabaseCommand.CommandText = queryString;
            this.DatabaseCommand.CommandType = CommandType.Text;
            this.BindQueryParameter(bindParam);
            return await this.RetrieveBlob(this.DatabaseCommand, stringPathDownload, stringCustomSingleFileName);
        }

        public CSqlite NewExternalConnection(string dbName) {
            var postgres = (CSqlite) this.Clone();
            postgres.InitializeSqliteDatabase(dbName);
            postgres.SettingUpDatabase();
            return postgres;
        }

    }

}
