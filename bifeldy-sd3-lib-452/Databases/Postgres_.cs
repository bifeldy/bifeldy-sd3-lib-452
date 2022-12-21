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
        Task<bool> BulkInsertInto(string tableName, DataTable dataTable);
    }

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

        // https://stackoverflow.com/questions/65687071/bulk-insert-copy-ienumerable-into-table-with-npgsql
        public async Task<bool> BulkInsertInto(string tableName, DataTable dataTable) {
            bool result = false;
            Exception exception = null;
            try {
                if (string.IsNullOrEmpty(tableName)) {
                    throw new Exception("No destination table is set ...");
                }
                int colCount = dataTable.Columns.Count;

                NpgsqlDbType[] types = new NpgsqlDbType[colCount];
                int[] lengths = new int[colCount];
                string[] fieldNames = new string[colCount];

                DatabaseCommand.CommandText = $"SELECT * FROM {tableName} LIMIT 1";
                using (NpgsqlDataReader rdr = (NpgsqlDataReader) await DatabaseCommand.ExecuteReaderAsync()) {
                    if (rdr.FieldCount != colCount) {
                        throw new Exception("Column count does not match ...");
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

    }

}
