﻿/**
 * 
 * Author       :: Basilius Bias Astho Christyono
 * Phone        :: (+62) 889 236 6466
 * 
 * Department   :: IT SD 03
 * Mail         :: bias@indomaret.co.id
 * 
 * Catatan      :: Class Database Bawaan
 *              :: Tidak Untuk Didaftarkan Ke DI Container
 *              :: Hanya Untuk Inherit
 *              :: Mohon & Harap Tidak Digunakan
 * 
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

using bifeldy_sd3_lib_452.Extensions;
using bifeldy_sd3_lib_452.Models;
using bifeldy_sd3_lib_452.Utilities;

namespace bifeldy_sd3_lib_452.Abstractions {

    public interface IDatabase {
        string DbUsername { get; } // Hanya Expose Get Saja
        string DbName { get; } // Hanya Expose Get Saja
        string DbConnectionString { get; } // Hanya Expose Get Saja
        bool Available { get; }
        bool HasUnCommitRollbackSqlQuery { get; }
        void CloseConnection(bool force = false);
        Task MarkBeforeCommitRollback();
        void MarkSuccessCommitAndClose();
        void MarkFailedRollbackAndClose();
        Task<DataColumnCollection> GetAllColumnTableAsync(string tableName);
        Task<DataTable> GetDataTableAsync(string queryString, List<CDbQueryParamBind> bindParam = null);
        Task<List<T>> GetListAsync<T>(string queryString, List<CDbQueryParamBind> bindParam = null);
        Task<T> ExecScalarAsync<T>(string queryString, List<CDbQueryParamBind> bindParam = null);
        Task<int> ExecQueryWithResultAsync(string queryString, List<CDbQueryParamBind> bindParam = null);
        Task<bool> ExecQueryAsync(string queryString, List<CDbQueryParamBind> bindParam = null, int minRowsAffected = 1, bool shouldEqualMinRowsAffected = false);
        Task<CDbExecProcResult> ExecProcedureAsync(string procedureName, List<CDbQueryParamBind> bindParam = null);
        Task<bool> BulkInsertInto(string tableName, DataTable dataTable);
        Task<string> BulkGetCsv(string queryString, string delimiter, string filename, List<CDbQueryParamBind> bindParam = null, string outputFolderPath = null, bool useRawQueryWithoutParam = false, bool includeHeader = true, bool useDoubleQuote = true, bool allUppercase = true, Encoding encoding = null);
        Task<DbDataReader> ExecReaderAsync(string queryString, List<CDbQueryParamBind> bindParam = null, CommandBehavior commandBehavior = CommandBehavior.Default);
        Task<List<string>> RetrieveBlob(string stringPathDownload, string queryString, List<CDbQueryParamBind> bindParam = null, string stringCustomSingleFileName = null, Encoding encoding = null);
    }

    public abstract class CDatabase : IDatabase, ICloneable {

        private readonly ILogger _logger;
        private readonly IConverter _converter;
        private readonly ICsv _csv;

        protected DbConnection DatabaseConnection { get; set; }
        protected DbTransaction DatabaseTransaction { get; set; }

        public string DbUsername { get; set; }
        public string DbPassword { get; set; }
        public string DbIpAddrss { get; set; }
        public string DbPort { get; set; }
        public string DbName { get; set; }
        public string DbTnsOdp { get; set; }
        public string DbConnectionString { get; set; }

        public bool Available => this.DatabaseConnection != null;
        public bool HasUnCommitRollbackSqlQuery => this.DatabaseTransaction != null;

        public CDatabase(ILogger logger, IConverter converter, ICsv csv) {
            this._logger = logger;
            this._converter = converter;
            this._csv = csv;
        }

        public object Clone() {
            return this.MemberwiseClone();
        }

        protected async Task OpenConnection() {
            if (!this.Available) {
                return;
            }

            if (!this.HasUnCommitRollbackSqlQuery) {
                if (this.DatabaseConnection.State == ConnectionState.Open) {
                    throw new Exception("Koneksi Database Sedang Digunakan");
                }

                await this.DatabaseConnection.OpenAsync();
            }
        }

        /// <summary> Jangan Lupa Di Commit Atau Rollback Sebelum Menjalankan Ini </summary>
        public void CloseConnection(bool force = false) {
            if (!this.Available) {
                return;
            }

            if (force) {
                if (this.HasUnCommitRollbackSqlQuery) {
                    this.DatabaseTransaction.Dispose();
                }

                this.DatabaseTransaction = null;
            }

            if (!this.HasUnCommitRollbackSqlQuery && this.DatabaseConnection.State == ConnectionState.Open) {
                this.DatabaseConnection.Close();
            }
        }

        public async Task MarkBeforeCommitRollback() {
            if (!this.Available) {
                return;
            }

            await this.OpenConnection();
            this.DatabaseTransaction = this.DatabaseConnection.BeginTransaction(IsolationLevel.ReadCommitted);
        }

        public void MarkSuccessCommitAndClose() {
            if (!this.Available) {
                return;
            }

            if (this.HasUnCommitRollbackSqlQuery) {
                this.DatabaseTransaction.Commit();
            }

            this.CloseConnection(true);
        }

        public void MarkFailedRollbackAndClose() {
            if (!this.Available) {
                return;
            }

            if (this.HasUnCommitRollbackSqlQuery) {
                this.DatabaseTransaction.Rollback();
            }

            this.CloseConnection(true);
        }

        protected void LogQueryParameter(DbCommand databaseCommand, char databaseParameterPrefix) {
            string sqlTextQueryParameters = databaseCommand.CommandText;
            for (int i = 0; i < databaseCommand.Parameters.Count; i++) {
                dynamic pVal = databaseCommand.Parameters[i].Value;

                Type pValType = Nullable.GetUnderlyingType(pVal?.GetType()) ?? pVal?.GetType();
                if (pValType == null || pValType == typeof(DBNull)) {
                    pVal = "NULL";
                }
                else if (pValType == typeof(string)) {
                    pVal = $"'{pVal}'";
                }
                else if (pValType == typeof(DateTime)) {
                    pVal = $"TO_TIMESTAMP('{((DateTime)pVal).ToLocalTime():yyyy-MM-dd HH:mm:ss}', 'yyyy-MM-dd HH24:mi:ss')";
                }

                var regex = new Regex($"{databaseParameterPrefix}{databaseCommand.Parameters[i].ParameterName}");
                sqlTextQueryParameters = regex.Replace(sqlTextQueryParameters, pVal.ToString(), 1);
            }

            sqlTextQueryParameters = sqlTextQueryParameters.Replace(Environment.NewLine, " ");
            sqlTextQueryParameters = Regex.Replace(sqlTextQueryParameters, @"\s+", " ");
            sqlTextQueryParameters = sqlTextQueryParameters.Trim();

            this._logger.WriteInfo(this.GetType().Name, sqlTextQueryParameters.Trim());
        }

        protected virtual async Task<DataColumnCollection> GetAllColumnTableAsync(string tableName, DbCommand databaseCommand) {
            DataTable dt = await this.GetDataTableAsync(databaseCommand);
            return dt.Columns;
        }

        protected virtual async Task<DataTable> GetDataTableAsync(DbCommand databaseCommand) {
            var result = new DataTable();
            Exception exception = null;
            try {
                // await OpenConnection();
                // dataAdapter.Fill(result);
                using (DbDataReader dr = await this.ExecReaderAsync(databaseCommand)) {
                    result.Load(dr);
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

        protected virtual async Task<List<T>> GetListAsync<T>(DbCommand databaseCommand) {
            var result = new List<T>();
            Exception exception = null;
            try {
                using (DbDataReader dr = await this.ExecReaderAsync(databaseCommand, CommandBehavior.SequentialAccess)) {
                    result = dr.ToList<T>();
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

        protected virtual async Task<T> ExecScalarAsync<T>(DbCommand databaseCommand) {
            T result = default;
            Exception exception = null;
            try {
                await this.OpenConnection();
                object _obj = await databaseCommand.ExecuteScalarAsync();
                if (_obj != null && _obj != DBNull.Value) {
                    result = this._converter.ObjectToT<T>(_obj);
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

        protected virtual async Task<int> ExecQueryWithResultAsync(DbCommand databaseCommand) {
            int affectedRows = 0;
            Exception exception = null;
            try {
                await this.OpenConnection();
                affectedRows = await databaseCommand.ExecuteNonQueryAsync();
            }
            catch (Exception ex) {
                this._logger.WriteError(ex, 4);
                exception = ex;
            }
            finally {
                this.CloseConnection();
            }

            return (exception == null) ? affectedRows : throw exception;
        }

        protected virtual async Task<bool> ExecQueryAsync(DbCommand databaseCommand, int minRowsAffected = 1, bool shouldEqualMinRowsAffected = false) {
            int affectedRows = await this.ExecQueryWithResultAsync(databaseCommand);
            return shouldEqualMinRowsAffected ? affectedRows == minRowsAffected : affectedRows >= minRowsAffected;
        }

        protected virtual async Task<CDbExecProcResult> ExecProcedureAsync(DbCommand databaseCommand) {
            var result = new CDbExecProcResult {
                STATUS = false,
                QUERY = databaseCommand.CommandText,
                PARAMETERS = databaseCommand.Parameters
            };
            Exception exception = null;
            try {
                await this.OpenConnection();
                result.STATUS = await databaseCommand.ExecuteNonQueryAsync() == -1;
                result.PARAMETERS = databaseCommand.Parameters;
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
        /// <summary> Saat Setelah Selesai Baca Dan Tidak Digunakan Lagi </summary>
        /// <summary> Bisa Pakai Manual Panggil Fungsi Close / Commit / Rollback Di Atas </summary>
        protected virtual async Task<DbDataReader> ExecReaderAsync(DbCommand databaseCommand, CommandBehavior commandBehavior = CommandBehavior.Default) {
            DbDataReader result = null;
            Exception exception = null;
            try {
                await this.OpenConnection();
                result = await databaseCommand.ExecuteReaderAsync(commandBehavior);
            }
            catch (Exception ex) {
                this._logger.WriteError(ex, 4);
                exception = ex;
            }
            finally {
                // Kalau Koneksinya Di Close Dari Sini Tidak Akan Bisa Pakai Reader Untuk Baca Lagi
                // this.CloseConnection();
            }

            return (exception == null) ? result : throw exception;
        }

        protected virtual async Task<List<string>> RetrieveBlob(DbCommand databaseCommand, string stringPathDownload, string stringFileName = null, Encoding encoding = null) {
            var result = new List<string>();
            Exception exception = null;
            try {
                string _oldCmdTxt = databaseCommand.CommandText;
                databaseCommand.CommandText = $"SELECT COUNT(*) FROM ( {_oldCmdTxt} ) RetrieveBlob_{DateTime.Now.Ticks}";
                ulong _totalFiles = await this.ExecScalarAsync<ulong>(databaseCommand);
                if (_totalFiles <= 0) {
                    throw new Exception("File Tidak Ditemukan");
                }

                databaseCommand.CommandText = _oldCmdTxt;
                using (DbDataReader rdrGetBlob = await this.ExecReaderAsync(databaseCommand, CommandBehavior.SequentialAccess)) {
                    if (string.IsNullOrEmpty(stringFileName) && rdrGetBlob.FieldCount != 2) {
                        throw new Exception($"Jika Nama File Kosong Maka Harus Berjumlah 2 Kolom{Environment.NewLine}SELECT kolom_blob_data, kolom_nama_file FROM ...");
                    }
                    else if (!string.IsNullOrEmpty(stringFileName) && rdrGetBlob.FieldCount > 1) {
                        throw new Exception($"Harus Berjumlah 1 Kolom{Environment.NewLine}SELECT kolom_blob_data FROM ...");
                    }

                    int bufferSize = 1024;
                    byte[] outByte = new byte[bufferSize];

                    while (await rdrGetBlob.ReadAsync()) {
                        string filePath = Path.Combine(stringPathDownload, stringFileName);

                        if (rdrGetBlob.FieldCount == 2) {
                            string fileMultipleName = rdrGetBlob.GetString(1);
                            if (string.IsNullOrEmpty(fileMultipleName)) {
                                fileMultipleName = $"{DateTime.Now.Ticks}";
                            }

                            filePath = Path.Combine(stringPathDownload, fileMultipleName);
                        }

                        using (var fs = new FileStream(filePath, FileMode.OpenOrCreate, FileAccess.Write)) {
                            using (var bw = new BinaryWriter(fs, encoding ?? Encoding.UTF8)) {
                                long startIndex = 0;
                                long retval = rdrGetBlob.GetBytes(0, startIndex, outByte, 0, bufferSize);

                                while (retval == bufferSize) {
                                    bw.Write(outByte);
                                    bw.Flush();
                                    startIndex += bufferSize;
                                    retval = rdrGetBlob.GetBytes(0, startIndex, outByte, 0, bufferSize);
                                }

                                if (retval > 0) {
                                    bw.Write(outByte, 0, (int) retval);
                                }

                                bw.Flush();
                            }
                        }

                        result.Add(filePath);
                    }
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

        public virtual async Task<string> BulkGetCsv(string queryString, string delimiter, string filename, List<CDbQueryParamBind> bindParam = null, string outputFolderPath = null, bool useRawQueryWithoutParamWithoutParam = false, bool includeHeader = true, bool useDoubleQuote = true, bool allUppercase = true, Encoding encoding = null) {
            string result = null;
            Exception exception = null;
            try {
                string path = Path.Combine(outputFolderPath ?? this._csv.CsvFolderPath, filename);
                if (File.Exists(path)) {
                    File.Delete(path);
                }

                string sqlQuery = $"SELECT * FROM ({queryString}) alias_{DateTime.Now.Ticks}";
                using (DbDataReader rdr = await this.ExecReaderAsync(sqlQuery, bindParam, CommandBehavior.SequentialAccess)) {
                    rdr.ToCsv(delimiter, path, includeHeader, useDoubleQuote, allUppercase, encoding ?? Encoding.UTF8);
                }

                result = path;
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

        /** Wajib di Override */

        protected abstract void BindQueryParameter(List<CDbQueryParamBind> parameters);

        public abstract Task<DataColumnCollection> GetAllColumnTableAsync(string tableName);
        public abstract Task<DataTable> GetDataTableAsync(string queryString, List<CDbQueryParamBind> bindParam = null);
        public abstract Task<List<T>> GetListAsync<T>(string queryString, List<CDbQueryParamBind> bindParam = null);
        public abstract Task<T> ExecScalarAsync<T>(string queryString, List<CDbQueryParamBind> bindParam = null);
        public abstract Task<int> ExecQueryWithResultAsync(string queryString, List<CDbQueryParamBind> bindParam = null);
        public abstract Task<bool> ExecQueryAsync(string queryString, List<CDbQueryParamBind> bindParam = null, int minRowsAffected = 1, bool shouldEqualMinRowsAffected = false);
        public abstract Task<CDbExecProcResult> ExecProcedureAsync(string procedureName, List<CDbQueryParamBind> bindParam = null);
        public abstract Task<bool> BulkInsertInto(string tableName, DataTable dataTable);
        public abstract Task<DbDataReader> ExecReaderAsync(string queryString, List<CDbQueryParamBind> bindParam = null, CommandBehavior commandBehavior = CommandBehavior.Default);
        public abstract Task<List<string>> RetrieveBlob(string stringPathDownload, string queryString, List<CDbQueryParamBind> bindParam = null, string stringCustomSingleFileName = null, Encoding encoding = null);

    }

}
