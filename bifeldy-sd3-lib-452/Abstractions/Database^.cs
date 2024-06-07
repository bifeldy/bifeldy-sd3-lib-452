/**
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
using System.Text.RegularExpressions;
using System.Threading.Tasks;

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
        Task<T> ExecScalarAsync<T>(string queryString, List<CDbQueryParamBind> bindParam = null);
        Task<bool> ExecQueryAsync(string queryString, List<CDbQueryParamBind> bindParam = null);
        Task<CDbExecProcResult> ExecProcedureAsync(string procedureName, List<CDbQueryParamBind> bindParam = null);
        Task<bool> BulkInsertInto(string tableName, DataTable dataTable);
        Task<DbDataReader> ExecReaderAsync(string queryString, List<CDbQueryParamBind> bindParam = null);
        Task<string> RetrieveBlob(string stringPathDownload, string stringFileName, string queryString, List<CDbQueryParamBind> bindParam = null);
    }

    public abstract class CDatabase : IDatabase, ICloneable {

        private readonly ILogger _logger;
        private readonly IConverter _converter;

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

        public CDatabase(ILogger logger, IConverter converter) {
            this._logger = logger;
            this._converter = converter;
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
                Type pValType = (pVal == null) ? typeof(string) : pVal.GetType();
                if (pValType == typeof(string) || pValType == typeof(DateTime)) {
                    pVal = $"'{pVal}'";
                }

                var regex = new Regex($"{databaseParameterPrefix}{databaseCommand.Parameters[i].ParameterName}");
                sqlTextQueryParameters = regex.Replace(sqlTextQueryParameters, pVal.ToString(), 1);
            }

            sqlTextQueryParameters = sqlTextQueryParameters.Replace($"\r\n", " ");
            sqlTextQueryParameters = Regex.Replace(sqlTextQueryParameters, @"\s+", " ");
            this._logger.WriteInfo(this.GetType().Name, sqlTextQueryParameters.Trim());
        }

        protected virtual async Task<DataColumnCollection> GetAllColumnTableAsync(string tableName, DbCommand databaseCommand) {
            DataTable dt = await this.GetDataTableAsync(databaseCommand);
            return dt.Columns;
        }

        protected virtual async Task<DataTable> GetDataTableAsync(DbCommand databaseCommand) {
            var result = new DataTable();
            DbDataReader dr = null;
            Exception exception = null;
            try {
                // await OpenConnection();
                // dataAdapter.Fill(result);
                dr = await this.ExecReaderAsync(databaseCommand);
                result.Load(dr);
            }
            catch (Exception ex) {
                this._logger.WriteError(ex, 4);
                exception = ex;
            }
            finally {
                if (dr != null) {
                    dr.Close();
                }

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
                    result = (T) Convert.ChangeType(_obj, typeof(T));
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

        protected virtual async Task<bool> ExecQueryAsync(DbCommand databaseCommand) {
            bool result = false;
            Exception exception = null;
            try {
                await this.OpenConnection();
                result = await databaseCommand.ExecuteNonQueryAsync() >= 0;
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
        protected virtual async Task<DbDataReader> ExecReaderAsync(DbCommand databaseCommand) {
            DbDataReader result = null;
            Exception exception = null;
            try {
                await this.OpenConnection();
                result = await databaseCommand.ExecuteReaderAsync();
            }
            catch (Exception ex) {
                this._logger.WriteError(ex, 4);
                exception = ex;
            }
            finally {
                // Kalau Koneksinya Di Close Dari Sini Tidak Akan Bisa Pakai Reader Untuk Baca Lagi
                // CloseConnection();
            }

            return (exception == null) ? result : throw exception;
        }

        protected virtual async Task<string> RetrieveBlob(DbCommand databaseCommand, string stringPathDownload, string stringFileName) {
            string result = null;
            Exception exception = null;
            try {
                await this.OpenConnection();
                string filePathResult = $"{stringPathDownload}/{stringFileName}";
                DbDataReader rdrGetBlob = await databaseCommand.ExecuteReaderAsync(CommandBehavior.SequentialAccess);
                if (!rdrGetBlob.HasRows) {
                    throw new Exception("File Tidak Ditemukan");
                }

                while (await rdrGetBlob.ReadAsync()) {
                    var fs = new FileStream(filePathResult, FileMode.OpenOrCreate, FileAccess.Write);
                    var bw = new BinaryWriter(fs);
                    long startIndex = 0;
                    int bufferSize = 8192;
                    byte[] outbyte = new byte[bufferSize - 1];
                    int retval = (int) rdrGetBlob.GetBytes(0, startIndex, outbyte, 0, bufferSize);
                    while (retval != bufferSize) {
                        bw.Write(outbyte);
                        bw.Flush();
                        Array.Clear(outbyte, 0, bufferSize);
                        startIndex += bufferSize;
                        retval = (int) rdrGetBlob.GetBytes(0, startIndex, outbyte, 0, bufferSize);
                    }

                    bw.Write(outbyte, 0, (retval > 0 ? retval : 1) - 1);
                    bw.Flush();
                    bw.Close();
                }

                rdrGetBlob.Close();
                rdrGetBlob = null;
                result = filePathResult;
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

        /** Wajib di Override */

        protected abstract void BindQueryParameter(List<CDbQueryParamBind> parameters);

        public abstract Task<DataColumnCollection> GetAllColumnTableAsync(string tableName);
        public abstract Task<DataTable> GetDataTableAsync(string queryString, List<CDbQueryParamBind> bindParam = null);
        public abstract Task<T> ExecScalarAsync<T>(string queryString, List<CDbQueryParamBind> bindParam = null);
        public abstract Task<bool> ExecQueryAsync(string queryString, List<CDbQueryParamBind> bindParam = null);
        public abstract Task<CDbExecProcResult> ExecProcedureAsync(string procedureName, List<CDbQueryParamBind> bindParam = null);
        public abstract Task<bool> BulkInsertInto(string tableName, DataTable dataTable);
        public abstract Task<DbDataReader> ExecReaderAsync(string queryString, List<CDbQueryParamBind> bindParam = null);
        public abstract Task<string> RetrieveBlob(string stringPathDownload, string stringFileName, string queryString, List<CDbQueryParamBind> bindParam = null);

    }

}
