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
            _app = app;
            _config = config;
            _logger = logger;

            InitializeSqliteDatabase();
            SettingUpDatabase();
        }

        private void InitializeSqliteDatabase(string dbName = null) {
            DbName = dbName ?? _config.Get<string>("LocalDbName", _app.GetConfig("local_db_name"));
        }

        private void SettingUpDatabase() {
            try {
                DbConnectionString = $"Data Source={DbName}";
                if (string.IsNullOrEmpty(DbName)) {
                    throw new Exception("Database Tidak Tersedia");
                }
                DatabaseConnection = new SQLiteConnection(DbConnectionString);
                DatabaseCommand = new SQLiteCommand {
                    Connection = (SQLiteConnection) DatabaseConnection,
                    CommandTimeout = 1800 // 30 menit
                };
                DatabaseAdapter = new SQLiteDataAdapter(DatabaseCommand);
                _logger.WriteInfo(GetType().Name, DbConnectionString);
            }
            catch (Exception ex) {
                _logger.WriteError(ex);
            }
        }

        protected override void BindQueryParameter(List<CDbQueryParamBind> parameters) {
            char prefix = ':';
            DatabaseCommand.Parameters.Clear();
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
                            DatabaseCommand.Parameters.Add(new SQLiteParameter {
                                ParameterName = $"{prefix}{pName}_{id}",
                                Value = data ?? DBNull.Value
                            });
                            id++;
                        }
                        Regex regex = new Regex($"{prefix}{pName}");
                        DatabaseCommand.CommandText = regex.Replace(DatabaseCommand.CommandText, bindStr, 1);
                    }
                    else {
                        SQLiteParameter param = new SQLiteParameter {
                            ParameterName = pName,
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
            LogQueryParameter(DatabaseCommand, prefix);
        }

        public override async Task<DataColumnCollection> GetAllColumnTableAsync(string tableName) {
            DatabaseCommand.CommandText = $@"SELECT * FROM {tableName} LIMIT 1";
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

                Type[] types = new Type[colCount];
                int[] lengths = new int[colCount];
                string[] fieldNames = new string[colCount];

                DatabaseCommand.CommandText = $"SELECT * FROM {tableName} LIMIT 1";
                using (SQLiteDataReader rdr = (SQLiteDataReader) await DatabaseCommand.ExecuteReaderAsync()) {
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

                List<CDbQueryParamBind> param = new List<CDbQueryParamBind>();
                StringBuilder sB = new StringBuilder($"INSERT INTO {tableName} (");

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
                await MarkBeforeCommitRollback();
                bool run = await ExecQueryAsync(query, param);
                MarkSuccessCommitAndClose();

                result = run;
            }
            catch (Exception ex) {
                MarkFailedRollbackAndClose();
                _logger.WriteError(ex, 3);
                exception = ex;
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

        public CSqlite NewExternalConnection(string dbName) {
            CSqlite postgres = (CSqlite) Clone();
            postgres.InitializeSqliteDatabase(dbName);
            postgres.SettingUpDatabase();
            return postgres;
        }

    }

}
