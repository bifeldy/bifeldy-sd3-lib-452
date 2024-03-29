﻿/**
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

        private NpgsqlCommand DatabaseCommand { get; set; }
        private NpgsqlDataAdapter DatabaseAdapter { get; set; }

        public CPostgres(IApplication app, ILogger logger, IConverter converter) : base(logger, converter) {
            _app = app;
            _logger = logger;

            InitializePostgresDatabase();
            SettingUpDatabase();
        }

        private void InitializePostgresDatabase(string dbIpAddrss = null, string dbPort = null, string dbUsername = null,  string dbPassword = null, string dbName = null) {
            DbIpAddrss = dbIpAddrss ?? _app.GetVariabel("IPPostgres");
            DbPort = dbPort ?? _app.GetVariabel("PortPostgres");
            DbUsername = dbUsername ?? _app.GetVariabel("UserPostgres");
            DbPassword = dbPassword ?? _app.GetVariabel("PasswordPostgres");
            DbName = dbName ?? _app.GetVariabel("DatabasePostgres");
        }

        private void SettingUpDatabase() {
            try {
                DbConnectionString = $"Host={DbIpAddrss};Port={DbPort};Username={DbUsername};Password={DbPassword};Database={DbName};Timeout=180;"; // 3 menit
                if (
                    string.IsNullOrEmpty(DbIpAddrss) ||
                    string.IsNullOrEmpty(DbPort) ||
                    string.IsNullOrEmpty(DbUsername) ||
                    string.IsNullOrEmpty(DbPassword) ||
                    string.IsNullOrEmpty(DbName)
                ) {
                    throw new Exception("Database Tidak Tersedia");
                }
                DatabaseConnection = new NpgsqlConnection(DbConnectionString);
                DatabaseCommand = new NpgsqlCommand {
                    Connection = (NpgsqlConnection) DatabaseConnection,
                    CommandTimeout = 1800 // 30 menit
                };
                DatabaseAdapter = new NpgsqlDataAdapter(DatabaseCommand);
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
                            bindStr += $"{prefix}{pName}_{id}";
                            DatabaseCommand.Parameters.Add(new NpgsqlParameter {
                                ParameterName = $"{pName}_{id}",
                                Value = data ?? DBNull.Value
                            });
                            id++;
                        }
                        Regex regex = new Regex($"{prefix}{pName}");
                        DatabaseCommand.CommandText = regex.Replace(DatabaseCommand.CommandText, bindStr, 1);
                    }
                    else {
                        NpgsqlParameter param = new NpgsqlParameter {
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
            string sqlTextQueryParameters = "(";
            if (bindParam != null) {
                for (int i = 0; i < bindParam.Count; i++) {
                    sqlTextQueryParameters += $":{bindParam[i].NAME}";
                    if (i + 1 < bindParam.Count) sqlTextQueryParameters += ",";
                }
            }
            sqlTextQueryParameters += ")";
            DatabaseCommand.CommandText = $"CALL {procedureName} {sqlTextQueryParameters}";
            DatabaseCommand.CommandType = CommandType.Text;
            BindQueryParameter(bindParam);
            return await ExecProcedureAsync(DatabaseCommand);
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

                NpgsqlDbType[] types = new NpgsqlDbType[colCount];
                int[] lengths = new int[colCount];
                string[] fieldNames = new string[colCount];

                DatabaseCommand.CommandText = $"SELECT * FROM {tableName} LIMIT 1";
                using (NpgsqlDataReader rdr = (NpgsqlDataReader) await DatabaseCommand.ExecuteReaderAsync()) {
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

                StringBuilder sB = new StringBuilder(fieldNames[0]);
                for (int p = 1; p < colCount; p++) {
                    sB.Append(", " + fieldNames[p]);
                }

                using (NpgsqlBinaryImporter writer = ((NpgsqlConnection) DatabaseConnection).BeginBinaryImport($"COPY {tableName} ({sB}) FROM STDIN (FORMAT BINARY)")) {
                    for (int j = 0; j < dataTable.Rows.Count; j++) {
                        DataRow dR = dataTable.Rows[j];
                        writer.StartRow();

                        for (int i = 0; i < colCount; i++) {
                            if (dR[i] == DBNull.Value) {
                                writer.WriteNull();
                            }
                            else {
                                switch (types[i]) {
                                    case NpgsqlDbType.Bigint:
                                        writer.Write((long) dR[i], types[i]);
                                        break;
                                    case NpgsqlDbType.Bit:
                                        if (lengths[i] > 1) {
                                            writer.Write((byte[]) dR[i], types[i]);
                                        }
                                        else {
                                            writer.Write((byte) dR[i], types[i]);
                                        }
                                        break;
                                    case NpgsqlDbType.Boolean:
                                        writer.Write((bool) dR[i], types[i]);
                                        break;
                                    case NpgsqlDbType.Bytea:
                                        writer.Write((byte[]) dR[i], types[i]);
                                        break;
                                    case NpgsqlDbType.Char:
                                        if (dR[i] is string) {
                                            writer.Write((string) dR[i], types[i]);
                                        }
                                        else if (dR[i] is Guid) {
                                            string value = dR[i].ToString();
                                            writer.Write(value, types[i]);
                                        }
                                        else if (lengths[i] > 1) {
                                            writer.Write((char[]) dR[i], types[i]);
                                        }
                                        else {
                                            char[] s = dR[i].ToString().ToCharArray();
                                            writer.Write(s[0], types[i]);
                                        }
                                        break;
                                    case NpgsqlDbType.Time:
                                    case NpgsqlDbType.Timestamp:
                                    case NpgsqlDbType.TimestampTz:
                                    case NpgsqlDbType.Date:
                                        writer.Write((DateTime) dR[i], types[i]);
                                        break;
                                    case NpgsqlDbType.Double:
                                        writer.Write((double) dR[i], types[i]);
                                        break;
                                    case NpgsqlDbType.Integer:
                                        try {
                                            if (dR[i] is int) {
                                                writer.Write((int) dR[i], types[i]);
                                                break;
                                            }
                                            else if (dR[i] is string) {
                                                int swap = Convert.ToInt32(dR[i]);
                                                writer.Write(swap, types[i]);
                                                break;
                                            }
                                        }
                                        catch (Exception ex) {
                                            _logger.WriteError(ex, 3);
                                            string sh = ex.Message;
                                        }
                                        writer.Write(dR[i], types[i]);
                                        break;
                                    case NpgsqlDbType.Interval:
                                        writer.Write((TimeSpan) dR[i], types[i]);
                                        break;
                                    case NpgsqlDbType.Numeric:
                                    case NpgsqlDbType.Money:
                                        writer.Write((decimal) dR[i], types[i]);
                                        break;
                                    case NpgsqlDbType.Real:
                                        writer.Write((float) dR[i], types[i]);
                                        break;
                                    case NpgsqlDbType.Smallint:
                                        try {
                                            if (dR[i] is byte) {
                                                var swap = Convert.ToInt16(dR[i]);
                                                writer.Write(swap, types[i]);
                                                break;
                                            }
                                            writer.Write((short) dR[i], types[i]);
                                        }
                                        catch (Exception ex) {
                                            _logger.WriteError(ex, 4);
                                            string ms = ex.Message;
                                        }
                                        break;
                                    case NpgsqlDbType.Varchar:
                                    case NpgsqlDbType.Text:
                                        writer.Write((string) dR[i], types[i]);
                                        break;
                                    case NpgsqlDbType.Uuid:
                                        writer.Write((Guid) dR[i], types[i]);
                                        break;
                                    case NpgsqlDbType.Xml:
                                        writer.Write((string) dR[i], types[i]);
                                        break;
                                }
                            }
                        }
                    }

                    writer.Complete();
                }

                result = true;
            }
            catch (Exception ex) {
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

        public CPostgres NewExternalConnection(string dbIpAddrss, string dbPort, string dbUsername, string dbPassword, string dbName) {
            CPostgres postgres = (CPostgres) Clone();
            postgres.InitializePostgresDatabase(dbIpAddrss, dbPort, dbUsername, dbPassword, dbName);
            postgres.SettingUpDatabase();
            return postgres;
        }

    }

}
