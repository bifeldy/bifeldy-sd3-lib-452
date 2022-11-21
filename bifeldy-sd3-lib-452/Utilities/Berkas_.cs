/**
 * 
 * Author       :: Basilius Bias Astho Christyono
 * Mail         :: bias@indomaret.co.id
 * Phone        :: (+62) 889 236 6466
 * 
 * Department   :: IT SD 03
 * Mail         :: bias@indomaret.co.id
 * 
 * Catatan      :: Mengatur Storage File & Folder
 *              :: Harap Didaftarkan Ke DI Container
 * 
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Text;

using Ionic.Zip;

namespace bifeldy_sd3_lib_452.Utilities {

    public interface IBerkas {
        int MaxOldRetentionDay { get; }
        string TempFolderPath { get; }
        string ZipFolderPath { get; }
        string DownloadFolderPath { get; }
        List<string> ListFileForTransfer { get; }
        void CleanUp();
        void DeleteOldFilesInFolder(string folderPath, int maxOldDays = 14);
        bool DataTable2CSV(DataTable table, string filename, string separator, string outputFolderPath = null);
        int ZipFileInTempFolder(string zipFileName);
    }

    public sealed class CBerkas : IBerkas {

        private readonly IApplication _app;
        private readonly ILogger _logger;

        public int MaxOldRetentionDay { get; }
        public string TempFolderPath { get; }
        public string ZipFolderPath { get; }
        public string DownloadFolderPath { get; }
        public List<string> ListFileForTransfer { get; }

        public CBerkas(IApplication app, ILogger logger) {
            _app = app;
            _logger = logger;

            ListFileForTransfer = new List<string>();
            MaxOldRetentionDay = 14;
            TempFolderPath = Path.Combine(_app.AppLocation, "Temp_Files");
            if (!Directory.Exists(TempFolderPath)) {
                Directory.CreateDirectory(TempFolderPath);
            }
            ZipFolderPath = Path.Combine(_app.AppLocation, "Zip_Files");
            if (!Directory.Exists(ZipFolderPath)) {
                Directory.CreateDirectory(ZipFolderPath);
            }
            DownloadFolderPath = Path.Combine(_app.AppLocation, "Download_Files");
            if (!Directory.Exists(DownloadFolderPath)) {
                Directory.CreateDirectory(DownloadFolderPath);
            }
        }

        public void CleanUp() {
            DeleteOldFilesInFolder(_logger.LogErrorFolderPath, MaxOldRetentionDay);
            DeleteOldFilesInFolder(TempFolderPath, MaxOldRetentionDay);
            DeleteOldFilesInFolder(ZipFolderPath, MaxOldRetentionDay);
        }

        public void DeleteOldFilesInFolder(string folderPath, int maxOldDays = 0) {
            string path = folderPath ?? TempFolderPath;
            try {
                if (Directory.Exists(path)) {
                    DirectoryInfo di = new DirectoryInfo(path);
                    FileSystemInfo[] fsis = di.GetFileSystemInfos();
                    foreach (FileSystemInfo fsi in fsis) {
                        if (fsi.Attributes == FileAttributes.Directory) {
                            DeleteOldFilesInFolder(fsi.FullName, maxOldDays);
                        }
                        if (fsi.LastWriteTime <= DateTime.Now.AddDays(-maxOldDays)) {
                            _logger.WriteLog($"{GetType().Name}DelFileDir", fsi.FullName);
                            fsi.Delete();
                        }
                    }
                }
            }
            catch (Exception ex) {
                _logger.WriteError(ex.Message);
            }
        }

        public bool DataTable2CSV(DataTable table, string filename, string separator, string outputFolderPath = null) {
            bool res = false;
            string path = Path.Combine(outputFolderPath ?? TempFolderPath, filename);
            StreamWriter writer = null;
            try {
                writer = new StreamWriter(path);
                string sep = "";
                StringBuilder builder = new StringBuilder();
                foreach (DataColumn col in table.Columns) {
                    builder.Append(sep).Append(col.ColumnName);
                    sep = separator;
                }
                writer.WriteLine(builder.ToString());
                foreach (DataRow row in table.Rows) {
                    sep = "";
                    builder = new StringBuilder();
                    foreach (DataColumn col in table.Columns) {
                        builder.Append(sep).Append(row[col.ColumnName]);
                        sep = separator;
                    }
                    writer.WriteLine(builder.ToString());
                }
                _logger.WriteLog($"{GetType().Name}Dt2Csv", path);
                res = true;
            }
            catch (Exception ex) {
                _logger.WriteError(ex.Message);
            }
            finally {
                if (writer != null) {
                    writer.Close();
                }
            }
            return res;
        }

        public int ZipFileInTempFolder(string zipFileName) {
            int result = 0;
            try {
                ZipFile zip = new ZipFile();
                foreach (string targetFileName in ListFileForTransfer) {
                    string filePath = Path.Combine(TempFolderPath, targetFileName);
                    zip.AddFile(filePath, "");
                    result++;
                    _logger.WriteLog($"{GetType().Name}ZipAdd", filePath);
                }
                string outputPath = Path.Combine(ZipFolderPath, zipFileName);
                zip.Save(outputPath);
                _logger.WriteLog($"{GetType().Name}ZipSave", outputPath);
            }
            catch (Exception ex) {
                _logger.WriteError(ex.Message);
            }
            return result;
        }

    }

}
