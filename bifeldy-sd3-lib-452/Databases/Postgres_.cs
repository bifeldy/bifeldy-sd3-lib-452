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
using System.Collections.ObjectModel;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

using Npgsql;
using NpgsqlTypes;
using Npgsql.Schema;

using bifeldy_sd3_lib_452.Abstractions;
using bifeldy_sd3_lib_452.Models;
using bifeldy_sd3_lib_452.Utilities;

namespace bifeldy_sd3_lib_452.Databases {

    public interface IPostgres : IDatabase {
        CPostgres NewExternalConnection(string dbIpAddrss, string dbPort, string dbUsername, string dbPassword, string dbName);
    }

    public sealed class CPostgres : CDatabase, IPostgres {

        private readonly IApplication _app;
        private readonly ILogger _logger;
        private readonly ICsv _csv;

        private NpgsqlCommand DatabaseCommand { get; set; }
        private NpgsqlDataAdapter DatabaseAdapter { get; set; }

        public CPostgres(IApplication app, ILogger logger, IConverter converter, ICsv csv) : base(logger, converter, csv) {
            this._app = app;
            this._logger = logger;
            this._csv = csv;

            this.InitializePostgresDatabase();
            this.SettingUpDatabase();
        }

        private void InitializePostgresDatabase(string dbIpAddrss = null, string dbPort = null, string dbUsername = null,  string dbPassword = null, string dbName = null) {
            this.DbIpAddrss = dbIpAddrss ?? this._app.GetVariabel("IPPostgres");
            this.DbPort = dbPort ?? this._app.GetVariabel("PortPostgres");
            this.DbUsername = dbUsername ?? this._app.GetVariabel("UserPostgres");
            this.DbPassword = dbPassword ?? this._app.GetVariabel("PasswordPostgres");
            this.DbName = dbName ?? this._app.GetVariabel("DatabasePostgres");
        }

        private void SettingUpDatabase() {
            try {
                this.DbConnectionString = $"Host={this.DbIpAddrss};Port={this.DbPort};Username={this.DbUsername};Password={this.DbPassword};Database={this.DbName};Timeout=180;"; // 3 menit
                if (
                    string.IsNullOrEmpty(this.DbIpAddrss) ||
                    string.IsNullOrEmpty(this.DbPort) ||
                    string.IsNullOrEmpty(this.DbUsername) ||
                    string.IsNullOrEmpty(this.DbPassword) ||
                    string.IsNullOrEmpty(this.DbName)
                ) {
                    throw new Exception("Database Tidak Tersedia");
                }

                this.DatabaseConnection = new NpgsqlConnection(this.DbConnectionString);
                if (this._app.DebugMode) {
                    ((NpgsqlConnection) this.DatabaseConnection).Notice += (_, evt) => {
                        this._logger.WriteInfo(this.GetType().Name, evt.Notice.MessageText);
                    };
                }

                this.DatabaseCommand = new NpgsqlCommand {
                    Connection = (NpgsqlConnection) this.DatabaseConnection,
                    CommandTimeout = 1800 // 30 menit
                };
                this.DatabaseAdapter = new NpgsqlDataAdapter(this.DatabaseCommand);
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
                            this.DatabaseCommand.Parameters.Add(new NpgsqlParameter {
                                ParameterName = $"{pName}_{id}",
                                Value = data ?? DBNull.Value
                            });
                            id++;
                        }

                        var regex = new Regex($"{prefix}{pName}");
                        this.DatabaseCommand.CommandText = regex.Replace(this.DatabaseCommand.CommandText, bindStr, 1);
                    }
                    else {
                        var param = new NpgsqlParameter {
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
            string sqlTextQueryParameters = "(";
            if (bindParam != null) {
                for (int i = 0; i < bindParam.Count; i++) {
                    sqlTextQueryParameters += $":{bindParam[i].NAME}";
                    if (i + 1 < bindParam.Count) {
                        sqlTextQueryParameters += ",";
                    }
                }
            }

            sqlTextQueryParameters += ")";
            this.DatabaseCommand.CommandText = $"CALL {procedureName} {sqlTextQueryParameters}";
            this.DatabaseCommand.CommandType = CommandType.Text;
            this.BindQueryParameter(bindParam);
            return await this.ExecProcedureAsync(this.DatabaseCommand);
        }

        // https://stackoverflow.com/questions/65687071/bulk-insert-copy-ienumerable-into-table-with-npgsql
        public override async Task<bool> BulkInsertInto(string tableName, DataTable dataTable) {
            bool result = false;
            Exception exception = null;
            try {
                if (string.IsNullOrEmpty(tableName)) {
                    throw new Exception("Target Tabel Tidak Ditemukan");
                }

                int colCount = dataTable.Columns.Count;

                var types = new NpgsqlDbType[colCount];
                int[] lengths = new int[colCount];
                string[] fieldNames = new string[colCount];

                this.DatabaseCommand.CommandText = $"SELECT * FROM {tableName} WHERE 1 = 0";
                using (var rdr = (NpgsqlDataReader) await this.ExecReaderAsync(this.DatabaseCommand)) {
                    if (rdr.FieldCount != colCount) {
                        throw new Exception("Jumlah Kolom Tabel Tidak Sama");
                    }

                    ReadOnlyCollection<NpgsqlDbColumn> columns = rdr.GetColumnSchema();
                    for (int i = 0; i < colCount; i++) {
                        types[i] = (NpgsqlDbType) columns[i].NpgsqlDbType;
                        lengths[i] = columns[i].ColumnSize == null ? 0 : (int) columns[i].ColumnSize;
                        fieldNames[i] = columns[i].ColumnName;
                    }
                }

                var sB = new StringBuilder(fieldNames[0]);
                for (int p = 1; p < colCount; p++) {
                    sB.Append(", " + fieldNames[p]);
                }

                using (NpgsqlBinaryImporter writer = ((NpgsqlConnection) this.DatabaseConnection).BeginBinaryImport($"COPY {tableName} ({sB}) FROM STDIN (FORMAT BINARY)")) {
                    for (int j = 0; j < dataTable.Rows.Count; j++) {
                        DataRow dR = dataTable.Rows[j];
                        writer.StartRow();

                        for (int i = 0; i < colCount; i++) {
                            if (dR[fieldNames[i]] == DBNull.Value) {
                                writer.WriteNull();
                            }
                            else {
                                dynamic _obj = dR[fieldNames[i]];
                                switch (types[i]) {
                                    case NpgsqlDbType.Bigint:
                                        writer.Write(Convert.ToInt64(_obj), types[i]);
                                        break;
                                    case NpgsqlDbType.Integer:
                                        writer.Write(Convert.ToInt32(_obj), types[i]);
                                        break;
                                    case NpgsqlDbType.Smallint:
                                        writer.Write(Convert.ToInt16(_obj), types[i]);
                                        break;
                                    case NpgsqlDbType.Money:
                                    case NpgsqlDbType.Numeric:
                                        writer.Write(Convert.ToDecimal(_obj), types[i]);
                                        break;
                                    case NpgsqlDbType.Double:
                                        writer.Write(Convert.ToDouble(_obj), types[i]);
                                        break;
                                    case NpgsqlDbType.Real:
                                        writer.Write(Convert.ToSingle(_obj), types[i]);
                                        break;
                                    case NpgsqlDbType.Boolean:
                                        writer.Write(Convert.ToBoolean(_obj), types[i]);
                                        break;
                                    case NpgsqlDbType.Char:
                                        if (lengths[i] == 1) {
                                            writer.Write(Convert.ToString(_obj).ToCharArray().First(), types[i]);
                                            break;
                                        }

                                        goto case NpgsqlDbType.Varchar;
                                    case NpgsqlDbType.Varchar:
                                    case NpgsqlDbType.Text:
                                        writer.Write(Convert.ToString(_obj), types[i]);
                                        break;
                                    case NpgsqlDbType.Time:
                                    case NpgsqlDbType.Timestamp:
                                    case NpgsqlDbType.TimestampTz:
                                    case NpgsqlDbType.Date:
                                        writer.Write(Convert.ToDateTime(_obj), types[i]);
                                        break;
                                    case NpgsqlDbType.Bytea:
                                        writer.Write((byte[]) _obj, types[i]);
                                        break;
                                    default:
                                        writer.Write(_obj, types[i]);
                                        break;
                                        // TODO :: Add More Handles While Free Time ~
                                }
                            }
                        }
                    }

                    writer.Complete();
                }

                result = true;
            }
            catch (Exception ex) {
                this._logger.WriteError(ex, 3);
                exception = ex;
            }
            finally {
                this.CloseConnection();
            }

            return (exception == null) ? result : throw exception;
        }

        public override async Task<string> BulkGetCsv(string rawQuery, string delimiter, string filename, string outputPath = null) {
            string result = null;
            Exception exception = null;
            try {
                string path = Path.Combine(outputPath ?? this._csv.CsvFolderPath, filename);
                if (File.Exists(path)) {
                    File.Delete(path);
                }
            
                if (string.IsNullOrEmpty(rawQuery) || string.IsNullOrEmpty(delimiter)) {
                    throw new Exception("Select Raw Query + Delimiter Harus Di Isi");
                }
            
                string sqlQuery = $"SELECT * FROM ({rawQuery}) alias_{DateTime.Now.Ticks} WHERE 1 = 0";
                sqlQuery = sqlQuery.Replace($"\r\n", " ");
                sqlQuery = Regex.Replace(sqlQuery, @"\s+", " ");
                this._logger.WriteInfo(this.GetType().Name, sqlQuery);
                using (var rdr = (NpgsqlDataReader) await this.ExecReaderAsync(sqlQuery)) {
                    ReadOnlyCollection<NpgsqlDbColumn> columns = rdr.GetColumnSchema();
                    string struktur = columns.Select(c => c.ColumnName).Aggregate((i, j) => $"{i}{delimiter}{j}");
                    using (var streamWriter = new StreamWriter(path, true)) {
                        streamWriter.WriteLine(struktur.ToUpper());
                        streamWriter.Flush();
                    }
                }
            
                sqlQuery = $"COPY ({rawQuery}) TO STDOUT WITH CSV DELIMITER '{delimiter}'";
                sqlQuery = sqlQuery.Replace($"\r\n", " ");
                sqlQuery = Regex.Replace(sqlQuery, @"\s+", " ");
                this._logger.WriteInfo(this.GetType().Name, sqlQuery);
            
                using (TextReader reader = ((NpgsqlConnection) this.DatabaseConnection).BeginTextExport(sqlQuery)) {
                    using (var streamWriter = new StreamWriter(path, true)) {
                        string line = null;
                        do {
                            line = reader.ReadLine()?.Trim();
                            if (!string.IsNullOrEmpty(line)) {
                                streamWriter.WriteLine(line.ToUpper());
                                streamWriter.Flush();
                            }
                        }
                        while (!string.IsNullOrEmpty(line));
                        result = path;
                    }
                }
            }
            catch (Exception ex) {
                this._logger.WriteError(ex, 3);
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

        public override async Task<List<string>> RetrieveBlob(string stringPathDownload, string queryString, List<CDbQueryParamBind> bindParam = null, string stringCustomSingleFileName = null) {
            this.DatabaseCommand.CommandText = queryString;
            this.DatabaseCommand.CommandType = CommandType.Text;
            this.BindQueryParameter(bindParam);
            return await this.RetrieveBlob(this.DatabaseCommand, stringPathDownload, stringCustomSingleFileName);
        }

        public CPostgres NewExternalConnection(string dbIpAddrss, string dbPort, string dbUsername, string dbPassword, string dbName) {
            var postgres = (CPostgres) this.Clone();
            postgres.InitializePostgresDatabase(dbIpAddrss, dbPort, dbUsername, dbPassword, dbName);
            postgres.SettingUpDatabase();
            return postgres;
        }

    }

}
