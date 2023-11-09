/**
 * 
 * Author       :: Basilius Bias Astho Christyono
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
using Ionic.Zlib;

namespace bifeldy_sd3_lib_452.Utilities {

    public interface IBerkas {
        int MaxOldRetentionDay { get; set; }
        string BackupFolderPath { get; }
        string TempFolderPath { get; }
        string ZipFolderPath { get; }
        string DownloadFolderPath { get; }
        List<string> ListFileForZip { get; }
        void CleanUp();
        void DeleteSingleFileInFolder(string fileName, string folderPath = null);
        void DeleteOldFilesInFolder(string folderPath, int maxOldDays, bool isInRecursive = false);
        bool DataTable2CSV(DataTable table, string filename, string separator, string outputFolderPath = null);
        int ZipListFileInFolder(string zipFileName, List<string> listFileName = null, string folderPath = null, string password = null);
        int ZipAllFileInFolder(string zipFileName, string folderPath = null, string password = null);
        void BackupAllFilesInTempFolder();
        void CopyAllFilesAndDirectories(DirectoryInfo source, DirectoryInfo target, bool isInRecursive = false);
    }

    public sealed class CBerkas : IBerkas {

        private readonly IApplication _app;
        private readonly ILogger _logger;
        private readonly IConfig _config;

        public int MaxOldRetentionDay { get; set; }

        public string BackupFolderPath { get; }
        public string TempFolderPath { get; }
        public string ZipFolderPath { get; }
        public string DownloadFolderPath { get; }

        public List<string> ListFileForZip { get; }

        public CBerkas(IApplication app, ILogger logger, IConfig config) {
            _app = app;
            _logger = logger;
            _config = config;

            ListFileForZip = new List<string>();

            MaxOldRetentionDay = _config.Get<int>("MaxOldRetentionDay", long.Parse(_app.GetConfig("max_old_retention_day")));

            BackupFolderPath = _config.Get<string>("BackupFolderPath", Path.Combine(_app.AppLocation, "_data", "Backup_Files"));
            if (!Directory.Exists(BackupFolderPath)) {
                Directory.CreateDirectory(BackupFolderPath);
            }

            TempFolderPath = _config.Get<string>("TempFolderPath", Path.Combine(_app.AppLocation, "_data", "Temp_Files"));
            if (!Directory.Exists(TempFolderPath)) {
                Directory.CreateDirectory(TempFolderPath);
            }

            ZipFolderPath = _config.Get<string>("ZipFolderPath", Path.Combine(_app.AppLocation, "_data", "Zip_Files"));
            if (!Directory.Exists(ZipFolderPath)) {
                Directory.CreateDirectory(ZipFolderPath);
            }

            DownloadFolderPath = _config.Get<string>("DownloadFolderPath", Path.Combine(_app.AppLocation, "_data", "Download_Files"));
            if (!Directory.Exists(DownloadFolderPath)) {
                Directory.CreateDirectory(DownloadFolderPath);
            }
        }

        public void CleanUp() {
            DeleteOldFilesInFolder(_logger.LogInfoFolderPath, MaxOldRetentionDay);
            DeleteOldFilesInFolder(_logger.LogErrorFolderPath, MaxOldRetentionDay);
            DeleteOldFilesInFolder(BackupFolderPath, MaxOldRetentionDay);
            DeleteOldFilesInFolder(TempFolderPath, MaxOldRetentionDay);
            DeleteOldFilesInFolder(ZipFolderPath, MaxOldRetentionDay);
            ListFileForZip.Clear();
        }

        public void DeleteSingleFileInFolder(string fileName, string folderPath = null) {
            string path = folderPath ?? TempFolderPath;
            try {
                FileInfo fi = new FileInfo(Path.Combine(path, fileName));
                if (fi.Exists) {
                    fi.Delete();
                }
            }
            catch (Exception ex) {
                _logger.WriteError(ex.Message);
            }
        }

        public void DeleteOldFilesInFolder(string folderPath, int maxOldDays, bool isInRecursive = false) {
            string path = folderPath ?? TempFolderPath;
            if (!isInRecursive && path == TempFolderPath) {
                BackupAllFilesInTempFolder();
            }
            try {
                if (Directory.Exists(path)) {
                    DirectoryInfo di = new DirectoryInfo(path);
                    FileSystemInfo[] fsis = di.GetFileSystemInfos();
                    foreach (FileSystemInfo fsi in fsis) {
                        if (fsi.Attributes == FileAttributes.Directory) {
                            DeleteOldFilesInFolder(fsi.FullName, maxOldDays, true);
                        }
                        if (fsi.LastWriteTime <= DateTime.Now.AddDays(-maxOldDays)) {
                            _logger.WriteInfo($"{GetType().Name}DelFileDir", fsi.FullName);
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
                string sep = string.Empty;
                StringBuilder builder = new StringBuilder();
                foreach (DataColumn col in table.Columns) {
                    builder.Append(sep).Append(col.ColumnName);
                    sep = separator;
                }
                // Untuk Export *.CSV Di Buat NAMA_KOLOM Besar Semua Tanpa Petik "NAMA_KOLOM"
                writer.WriteLine(builder.ToString().ToUpper());
                foreach (DataRow row in table.Rows) {
                    sep = string.Empty;
                    builder = new StringBuilder();
                    foreach (DataColumn col in table.Columns) {
                        builder.Append(sep).Append(row[col.ColumnName]);
                        sep = separator;
                    }
                    writer.WriteLine(builder.ToString());
                }
                _logger.WriteInfo($"{GetType().Name}Dt2Csv", path);
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

        public void CopyAllFilesAndDirectories(DirectoryInfo source, DirectoryInfo target, bool isInRecursive = false) {
            Directory.CreateDirectory(target.FullName);
            foreach (FileInfo fi in source.GetFiles()) {
                FileInfo res = fi.CopyTo(Path.Combine(target.FullName, fi.Name), true);
                _logger.WriteInfo($"{GetType().Name}CopyAndReplace", $"{fi.FullName} => {res.FullName}");
            }
            foreach (DirectoryInfo diSourceSubDir in source.GetDirectories()) {
                DirectoryInfo nextTargetSubDir = target.CreateSubdirectory(diSourceSubDir.Name);
                CopyAllFilesAndDirectories(diSourceSubDir, nextTargetSubDir, true);
            }
        }

        public void BackupAllFilesInTempFolder() {
            DirectoryInfo diSource = new DirectoryInfo(TempFolderPath);
            DirectoryInfo diTarget = new DirectoryInfo(BackupFolderPath);
            CopyAllFilesAndDirectories(diSource, diTarget);
        }

        public int ZipListFileInFolder(string zipFileName, List<string> listFileName = null, string folderPath = null, string password = null) {
            int totalFileInZip = 0;
            List<string> ls = listFileName ?? ListFileForZip;
            try {
                ZipFile zip = new ZipFile();
                if (!string.IsNullOrEmpty(password)) {
                    zip.Password = password;
                    zip.CompressionLevel = CompressionLevel.BestCompression;
                }
                foreach (string targetFileName in ls) {
                    string filePath = Path.Combine(folderPath ?? TempFolderPath, targetFileName);
                    ZipEntry zipEntry = zip.AddFile(filePath, "");
                    if (zipEntry != null) {
                        totalFileInZip++;
                    }
                    _logger.WriteInfo($"{GetType().Name}ZipAdd{(zipEntry == null ? "Fail" : "Ok")}", filePath);
                }
                string outputPath = Path.Combine(ZipFolderPath, zipFileName);
                zip.Save(outputPath);
                _logger.WriteInfo($"{GetType().Name}ZipSave", outputPath);
            }
            catch (Exception ex) {
                _logger.WriteError(ex.Message);
            }
            finally {
                if (listFileName == null) {
                    ls.Clear();
                }
            }
            return totalFileInZip;
        }

        public int ZipAllFileInFolder(string zipFileName, string folderPath = null, string password = null) {
            int totalFileInZip = 0;
            try {
                ZipFile zip = new ZipFile();
                if (!string.IsNullOrEmpty(password)) {
                    zip.Password = password;
                    zip.CompressionLevel = CompressionLevel.BestCompression;
                }
                DirectoryInfo directoryInfo = new DirectoryInfo(folderPath ?? TempFolderPath);
                FileInfo[] fileInfos = directoryInfo.GetFiles();
                foreach (FileInfo fileInfo in fileInfos) {
                    ZipEntry zipEntry = zip.AddFile(fileInfo.FullName, "");
                    if (zipEntry != null) {
                        totalFileInZip++;
                    }
                    _logger.WriteInfo($"{GetType().Name}ZipAdd{(zipEntry == null ? "Fail" : "Ok")}", fileInfo.FullName);
                }
                string outputPath = Path.Combine(ZipFolderPath, zipFileName);
                zip.Save(outputPath);
                _logger.WriteInfo($"{GetType().Name}ZipSave", outputPath);
            }
            catch (Exception ex) {
                _logger.WriteError(ex.Message);
            }
            return totalFileInZip;
        }

    }

}
