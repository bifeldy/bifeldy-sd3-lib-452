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
using System.Threading.Tasks;

using bifeldy_sd3_lib_452.Models;
using bifeldy_sd3_lib_452.Utilities;

namespace bifeldy_sd3_lib_452.Abstractions {

    public interface IDatabase {
        Task<(DataTable, Exception)> GetDataTableAsync(string queryString, List<CDbQueryParamBind> bindParam = null, bool closeConnection = true);
        Task<(T, Exception)> ExecScalarAsync<T>(string queryString, List<CDbQueryParamBind> bindParam = null, bool closeConnection = true);
        Task<(bool, Exception)> ExecQueryAsync(string queryString, List<CDbQueryParamBind> bindParam = null, bool closeConnection = true);
        Task<(CDbExecProcResult, Exception)> ExecProcedureAsync(string procedureName, List<CDbQueryParamBind> bindParam = null, bool closeConnection = true);
        Task<(int, Exception)> UpdateTable(DataSet dataSet, string dataSetTableName, string queryString, List<CDbQueryParamBind> bindParam = null, bool closeConnection = true);
        Task<(DbDataReader, Exception)> ExecReaderAsync(string queryString, List<CDbQueryParamBind> bindParam = null, bool closeConnection = false);
        Task<(string, Exception)> RetrieveBlob(string stringPathDownload, string stringFileName, string queryString, List<CDbQueryParamBind> bindParam = null, bool closeConnection = false);
        bool Available { get; }
        string DbName { get; set; }
        Task MarkBeforeExecQueryCommitAndRollback();
        void MarkSuccessExecQueryAndCommit();
        void MarkFailExecQueryAndRollback();
        void CloseConnection();
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

        public bool Available {
            get {
                return DatabaseConnection != null;
            }
        }

        public CDatabase(ILogger logger) {
            _logger = logger;
        }

        public void CloseConnection() {
            if (DatabaseConnection.State == ConnectionState.Open) {
                DatabaseConnection.Close();
            }
            if (DatabaseTransaction != null) {
                DatabaseTransaction.Dispose();
                DatabaseTransaction = null;
            }
        }

        public async Task MarkBeforeExecQueryCommitAndRollback() {
            if (DatabaseConnection.State != ConnectionState.Open) {
                await DatabaseConnection.OpenAsync();
            }
            DatabaseTransaction = DatabaseConnection.BeginTransaction(IsolationLevel.ReadCommitted);
        }

        public void MarkSuccessExecQueryAndCommit() {
            if (DatabaseTransaction != null) {
                DatabaseTransaction.Commit();
            }
            CloseConnection();
        }

        public void MarkFailExecQueryAndRollback() {
            if (DatabaseTransaction != null) {
                DatabaseTransaction.Rollback();
            }
            CloseConnection();
        }

        protected virtual async Task<(DataTable, Exception)> GetDataTableAsync(DbDataAdapter dataAdapter, bool autoCloseConnection = true) {
            DataTable dataTable = new DataTable();
            Exception exception = null;
            try {
                if (DatabaseConnection.State == ConnectionState.Open) {
                    throw new Exception("Database Connection Already In Use!");
                }
                else {
                    await DatabaseConnection.OpenAsync();
                    dataAdapter.Fill(dataTable);
                }
            }
            catch (Exception ex) {
                _logger.WriteError(ex, 4);
                exception = ex;
            }
            finally {
                if (autoCloseConnection) {
                    DatabaseConnection.Close();
                }
            }
            return (dataTable, exception);
        }

        protected virtual async Task<(T, Exception)> ExecScalarAsync<T>(DbCommand databaseCommand, bool autoCloseConnection = true) {
            T result = (T) Convert.ChangeType(null, typeof(T));
            Exception exception = null;
            try {
                if (DatabaseConnection.State == ConnectionState.Open) {
                    throw new Exception("Database Connection Already In Use!");
                }
                else {
                    await DatabaseConnection.OpenAsync();
                    object _obj = await databaseCommand.ExecuteScalarAsync();
                    result = (T) Convert.ChangeType(_obj, typeof(T));
                }
            }
            catch (Exception ex) {
                _logger.WriteError(ex, 4);
                exception = ex;
            }
            finally {
                if (autoCloseConnection) {
                    DatabaseConnection.Close();
                }
            }
            return (result, exception);
        }

        // Harap Jalankan `await MarkBeforeCommitAndRollback();` Telebih Dahulu Jika `autoCloseConnection = false`
        // Lalu Bisa Menjalankan `ExecQueryAsync();` Berkali - Kali (Dengan Koneksi Yang Sama)
        // Setelah Selesai Panggil `MarkSuccessExecQueryAndCommit();` atau `MarkFailExecQueryAndRollback();` Jika Gagal
        protected virtual async Task<(bool, Exception)> ExecQueryAsync(DbCommand databaseCommand, bool autoCloseConnection = true) {
            bool result = false;
            Exception exception = null;
            try {
                if (autoCloseConnection) {
                    if (DatabaseConnection.State == ConnectionState.Open) {
                        throw new Exception("Database Connection Already In Use!");
                    }
                    await DatabaseConnection.OpenAsync();
                }
                result = await databaseCommand.ExecuteNonQueryAsync() >= 0;
            }
            catch (Exception ex) {
                _logger.WriteError(ex, 4);
                exception = ex;
            }
            finally {
                if (autoCloseConnection) {
                    DatabaseConnection.Close();
                }
            }
            return (result, exception);
        }

        protected virtual async Task<(CDbExecProcResult, Exception)> ExecProcedureAsync(DbCommand databaseCommand, bool autoCloseConnection = true) {
            CDbExecProcResult result = new CDbExecProcResult {
                STATUS = false,
                QUERY = databaseCommand.CommandText,
                PARAMETERS = databaseCommand.Parameters
            };
            Exception exception = null;
            try {
                if (DatabaseConnection.State == ConnectionState.Open) {
                    throw new Exception("Database Connection Already In Use!");
                }
                else {
                    await DatabaseConnection.OpenAsync();
                    result.STATUS = await databaseCommand.ExecuteNonQueryAsync() == -1;
                    result.PARAMETERS = databaseCommand.Parameters;
                }
            }
            catch (Exception ex) {
                _logger.WriteError(ex, 4);
                exception = ex;
            }
            finally {
                if (autoCloseConnection) {
                    DatabaseConnection.Close();
                }
            }
            return (result, exception);
        }

        protected virtual async Task<(int, Exception)> UpdateTable(DbDataAdapter dataAdapter, DataSet dataSet, string dataSetTableName, bool autoCloseConnection = true) {
            int result = 0;
            Exception exception = null;
            try {
                if (DatabaseConnection.State == ConnectionState.Open) {
                    throw new Exception("Database Connection Already In Use!");
                }
                else {
                    await DatabaseConnection.OpenAsync();
                    result = dataAdapter.Update(dataSet, dataSetTableName);
                }
            }
            catch (Exception ex) {
                _logger.WriteError(ex, 4);
                exception = ex;
            }
            finally {
                if (autoCloseConnection) {
                    DatabaseConnection.Close();
                }
            }
            return (result, exception);
        }

        protected virtual async Task<(DbDataReader, Exception)> ExecReaderAsync(DbCommand databaseCommand, bool autoCloseConnection = false) {
            DbDataReader result = null;
            Exception exception = null;
            try {
                if (DatabaseConnection.State == ConnectionState.Open) {
                    throw new Exception("Database Connection Already In Use!");
                }
                else {
                    await DatabaseConnection.OpenAsync();
                    result = await databaseCommand.ExecuteReaderAsync();
                }
            }
            catch (Exception ex) {
                _logger.WriteError(ex, 4);
            }
            finally {
                if (autoCloseConnection) {
                    DatabaseConnection.Close();
                }
            }
            return (result, exception);
        }

        protected virtual async Task<(string, Exception)> RetrieveBlob(DbCommand databaseCommand, string stringPathDownload, string stringFileName, bool autoCloseConnection = false) {
            string result = null;
            Exception exception = null;
            try {
                if (DatabaseConnection.State == ConnectionState.Open) {
                    throw new Exception("Database Connection Already In Use!");
                }
                else {
                    await DatabaseConnection.OpenAsync();
                    DbDataReader rdrGetBlob = await databaseCommand.ExecuteReaderAsync(CommandBehavior.SequentialAccess);
                    if (!rdrGetBlob.HasRows) {
                        throw new Exception("Error file not found");
                    }
                    while (await rdrGetBlob.ReadAsync()) {
                        FileStream fs = new FileStream($"{stringPathDownload}/{stringFileName}", FileMode.OpenOrCreate, FileAccess.Write);
                        BinaryWriter bw = new BinaryWriter(fs);
                        long startIndex = 0;
                        int bufferSize = 8192;
                        byte[] outbyte = new byte[bufferSize - 1];
                        int retval = (int)rdrGetBlob.GetBytes(0, startIndex, outbyte, 0, bufferSize);
                        while (retval != bufferSize) {
                            bw.Write(outbyte);
                            bw.Flush();
                            Array.Clear(outbyte, 0, bufferSize);
                            startIndex += bufferSize;
                            retval = (int)rdrGetBlob.GetBytes(0, startIndex, outbyte, 0, bufferSize);
                        }
                        bw.Write(outbyte, 0, (retval > 0 ? retval : 1) - 1);
                        bw.Flush();
                        bw.Close();
                    }
                    rdrGetBlob.Close();
                    rdrGetBlob = null;
                    result = $"{stringPathDownload}/{stringFileName}";
                }
            }
            catch (Exception ex) {
                _logger.WriteError(ex, 4);
                exception = ex;
            }
            finally {
                if (autoCloseConnection) {
                    DatabaseConnection.Close();
                }
            }
            return (result, exception);
        }

        /** Wajib di Override */

        public abstract Task<(DataTable, Exception)> GetDataTableAsync(string queryString, List<CDbQueryParamBind> bindParam = null, bool closeConnection = true);
        public abstract Task<(T, Exception)> ExecScalarAsync<T>(string queryString, List<CDbQueryParamBind> bindParam = null, bool closeConnection = true);
        public abstract Task<(bool, Exception)> ExecQueryAsync(string queryString, List<CDbQueryParamBind> bindParam = null, bool closeConnection = true);
        public abstract Task<(CDbExecProcResult, Exception)> ExecProcedureAsync(string procedureName, List<CDbQueryParamBind> bindParam = null, bool closeConnection = true);
        public abstract Task<(int, Exception)> UpdateTable(DataSet dataSet, string dataSetTableName, string queryString, List<CDbQueryParamBind> bindParam = null, bool closeConnection = true);
        public abstract Task<(DbDataReader, Exception)> ExecReaderAsync(string queryString, List<CDbQueryParamBind> bindParam = null, bool closeConnection = false);
        public abstract Task<(string, Exception)> RetrieveBlob(string stringPathDownload, string stringFileName, string queryString, List<CDbQueryParamBind> bindParam = null, bool closeConnection = false);

    }

}
