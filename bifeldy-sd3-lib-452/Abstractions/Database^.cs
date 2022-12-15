/**
 * 
 * Author       :: Basilius Bias Astho Christyono
 * Mail         :: bias@indomaret.co.id
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
        Task<DataTable> GetDataTableAsync(string queryString, List<CDbQueryParamBind> bindParam = null);
        Task<T> ExecScalarAsync<T>(string queryString, List<CDbQueryParamBind> bindParam = null);
        Task<bool> ExecQueryAsync(string queryString, List<CDbQueryParamBind> bindParam = null);
        Task<CDbExecProcResult> ExecProcedureAsync(string procedureName, List<CDbQueryParamBind> bindParam = null);
        Task<int> UpdateTable(DataSet dataSet, string dataSetTableName, string queryString, List<CDbQueryParamBind> bindParam = null);
        Task<DbDataReader> ExecReaderAsync(string queryString, List<CDbQueryParamBind> bindParam = null);
        Task<string> RetrieveBlob(string stringPathDownload, string stringFileName, string queryString, List<CDbQueryParamBind> bindParam = null);
        string DbName { get; set; }
        bool Available { get; }
        bool HasUnCommitRollbackSqlQuery { get; }
        Task MarkBeforeCommitRollback();
        void MarkSuccessCommitAndClose();
        void MarkFailedRollbackAndClose();
        void CloseConnection(bool force = false);
    }

    public abstract class CDatabase : IDatabase {

        private readonly ILogger _logger;

        protected DbConnection DatabaseConnection { get; set; }
        protected DbTransaction DatabaseTransaction { get; set; }

        public string DbUsername { get; set; }
        public string DbPassword { get; set; }
        public string DbIpAddrss { get; set; }
        public string DbPort { get; set; }
        public string DbName { get; set; }
        public string DbTnsOdp { get; set; }
        public string DbConnectionString { get; set; }

        public bool Available => DatabaseConnection != null;
        public bool HasUnCommitRollbackSqlQuery => DatabaseTransaction != null;

        public CDatabase(ILogger logger) {
            _logger = logger;
        }

        public void CloseConnection(bool force = false) {
            if (force) {
                if (HasUnCommitRollbackSqlQuery) {
                    DatabaseTransaction.Dispose();
                }
                DatabaseTransaction = null;
            }
            if (!HasUnCommitRollbackSqlQuery && DatabaseConnection.State == ConnectionState.Open) {
                DatabaseConnection.Close();
            }
        }

        public async Task MarkBeforeCommitRollback() {
            if (DatabaseConnection.State != ConnectionState.Open) {
                await DatabaseConnection.OpenAsync();
            }
            DatabaseTransaction = DatabaseConnection.BeginTransaction(IsolationLevel.ReadCommitted);
        }

        public void MarkSuccessCommitAndClose() {
            if (HasUnCommitRollbackSqlQuery) {
                DatabaseTransaction.Commit();
            }
            CloseConnection(true);
        }

        public void MarkFailedRollbackAndClose() {
            if (HasUnCommitRollbackSqlQuery) {
                DatabaseTransaction.Rollback();
            }
            CloseConnection(true);
        }

        private async Task OpenConnection() {
            if (!HasUnCommitRollbackSqlQuery) {
                if (DatabaseConnection.State == ConnectionState.Open) {
                    throw new Exception("Database Connection Already In Use!");
                }
                await DatabaseConnection.OpenAsync();
            }
        }

        protected void LogQueryParameter(DbCommand databaseCommand) {
            string sqlTextQueryParameters = databaseCommand.CommandText;
            for (int i = 0; i < databaseCommand.Parameters.Count; i++) {
                object val = databaseCommand.Parameters[i].Value;
                if (databaseCommand.Parameters[i].Value.GetType() == typeof(string) || databaseCommand.Parameters[i].Value.GetType() == typeof(DateTime)) {
                    val = $"'{databaseCommand.Parameters[i].Value}'";
                }
                sqlTextQueryParameters = sqlTextQueryParameters.Replace($":{databaseCommand.Parameters[i].ParameterName}", val.ToString());
            }
            sqlTextQueryParameters = sqlTextQueryParameters.Replace($"\r\n", " ");
            sqlTextQueryParameters = Regex.Replace(sqlTextQueryParameters, @"\s+", " ");
            _logger.WriteLog(GetType().Name, sqlTextQueryParameters.Trim());
        }

        protected virtual async Task<DataTable> GetDataTableAsync(DbDataAdapter dataAdapter) {
            DataTable result = new DataTable();
            Exception exception = null;
            try {
                await OpenConnection();
                dataAdapter.Fill(result);
            }
            catch (Exception ex) {
                _logger.WriteError(ex, 4);
                exception = ex;
            }
            finally {
                CloseConnection();
            }
            return (exception == null) ? result : throw exception;
        }

        protected virtual async Task<T> ExecScalarAsync<T>(DbCommand databaseCommand) {
            dynamic x = null;
            switch (Type.GetTypeCode(typeof(T))) {
                case TypeCode.DateTime:
                    x = DateTime.MinValue;
                    break;
                case TypeCode.Byte:
                case TypeCode.SByte:
                case TypeCode.Int16:
                case TypeCode.UInt16:
                case TypeCode.Int32:
                case TypeCode.UInt32:
                case TypeCode.Int64:
                case TypeCode.UInt64:
                case TypeCode.Decimal:
                case TypeCode.Double:
                    x = 0;
                    break;
                case TypeCode.Boolean:
                    x = false;
                    break;
            }
            T result = (T) Convert.ChangeType(x, typeof(T));
            Exception exception = null;
            try {
                await OpenConnection();
                object _obj = await databaseCommand.ExecuteScalarAsync();
                result = (T) Convert.ChangeType(_obj, typeof(T));
            }
            catch (Exception ex) {
                _logger.WriteError(ex, 4);
                exception = ex;
            }
            finally {
                CloseConnection();
            }
            return (exception == null) ? result : throw exception;
        }

        protected virtual async Task<bool> ExecQueryAsync(DbCommand databaseCommand) {
            bool result = false;
            Exception exception = null;
            try {
                await OpenConnection();
                result = await databaseCommand.ExecuteNonQueryAsync() >= 0;
            }
            catch (Exception ex) {
                _logger.WriteError(ex, 4);
                exception = ex;
            }
            finally {
                CloseConnection();
            }
            return (exception == null) ? result : throw exception;
        }

        protected virtual async Task<CDbExecProcResult> ExecProcedureAsync(DbCommand databaseCommand) {
            CDbExecProcResult result = new CDbExecProcResult {
                STATUS = false,
                QUERY = databaseCommand.CommandText,
                PARAMETERS = databaseCommand.Parameters
            };
            Exception exception = null;
            try {
                await OpenConnection();
                result.STATUS = await databaseCommand.ExecuteNonQueryAsync() == -1;
                result.PARAMETERS = databaseCommand.Parameters;
            }
            catch (Exception ex) {
                _logger.WriteError(ex, 4);
                exception = ex;
            }
            finally {
                CloseConnection();
            }
            return (exception == null) ? result : throw exception;
        }

        protected virtual async Task<int> UpdateTable(DbDataAdapter dataAdapter, DataSet dataSet, string dataSetTableName) {
            int result = 0;
            Exception exception = null;
            try {
                await OpenConnection();
                result = dataAdapter.Update(dataSet, dataSetTableName);
            }
            catch (Exception ex) {
                _logger.WriteError(ex, 4);
                exception = ex;
            }
            finally {
                CloseConnection();
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
                await OpenConnection();
                result = await databaseCommand.ExecuteReaderAsync();
            }
            catch (Exception ex) {
                _logger.WriteError(ex, 4);
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
                await OpenConnection();
                string filePathResult = $"{stringPathDownload}/{stringFileName}";
                DbDataReader rdrGetBlob = await databaseCommand.ExecuteReaderAsync(CommandBehavior.SequentialAccess);
                if (!rdrGetBlob.HasRows) {
                    throw new Exception("Error file not found");
                }
                while (await rdrGetBlob.ReadAsync()) {
                    FileStream fs = new FileStream(filePathResult, FileMode.OpenOrCreate, FileAccess.Write);
                    BinaryWriter bw = new BinaryWriter(fs);
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
                _logger.WriteError(ex, 4);
                exception = ex;
            }
            finally {
                CloseConnection();
            }
            return (exception == null) ? result : throw exception;
        }

        /** Wajib di Override */

        public abstract Task<DataTable> GetDataTableAsync(string queryString, List<CDbQueryParamBind> bindParam = null);
        public abstract Task<T> ExecScalarAsync<T>(string queryString, List<CDbQueryParamBind> bindParam = null);
        public abstract Task<bool> ExecQueryAsync(string queryString, List<CDbQueryParamBind> bindParam = null);
        public abstract Task<CDbExecProcResult> ExecProcedureAsync(string procedureName, List<CDbQueryParamBind> bindParam = null);
        public abstract Task<int> UpdateTable(DataSet dataSet, string dataSetTableName, string queryString, List<CDbQueryParamBind> bindParam = null);
        public abstract Task<DbDataReader> ExecReaderAsync(string queryString, List<CDbQueryParamBind> bindParam = null);
        public abstract Task<string> RetrieveBlob(string stringPathDownload, string stringFileName, string queryString, List<CDbQueryParamBind> bindParam = null);

    }

}
