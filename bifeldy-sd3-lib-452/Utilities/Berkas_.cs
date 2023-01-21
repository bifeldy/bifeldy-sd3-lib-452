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
        int MaxOldRetentionDay { get; set; }
        string TempFolderPath { get; set; }
        string ZipFolderPath { get; set; }
        string DownloadFolderPath { get; set; }
        List<string> ListFileForZip { get; }
        void CleanUp();
        void DeleteSingleFileInFolder(string fileName, string folderPath = null);
        void DeleteOldFilesInFolder(string folderPath, int maxOldDays, bool isInRecursive = false);
        bool DataTable2CSV(DataTable table, string filename, string separator, string outputFolderPath = null);
        int ZipListFileInFolder(string zipFileName, List<string> listFileName = null, string folderPath = null);
        int ZipAllFileInFolder(string zipFileName, string folderPath = null);
        void BackupAllFilesInTempFolder();
        void CopyAllFilesAndDirectories(DirectoryInfo source, DirectoryInfo target, bool isInRecursive = false);
    }

    public sealed class CBerkas : IBerkas {

        private readonly IApplication _app;
        private readonly ILogger _logger;

        private string BackupFolderPath { get; }

        public int MaxOldRetentionDay { get; set; }

        public string TempFolderPath { get; set; }
        public string ZipFolderPath { get; set; }
        public string DownloadFolderPath { get; set; }

        public List<string> ListFileForZip { get; }

        public CBerkas(IApplication app, ILogger logger) {
            _app = app;
            _logger = logger;

            ListFileForZip = new List<string>();

            BackupFolderPath = Path.Combine(_app.AppLocation, "Backup_Files");
            if (!Directory.Exists(BackupFolderPath)) {
                Directory.CreateDirectory(BackupFolderPath);
            }

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
                string sep = "";
                StringBuilder builder = new StringBuilder();
                foreach (DataColumn col in table.Columns) {
                    builder.Append(sep).Append(col.ColumnName);
                    sep = separator;
                }
                // Untuk Export *.CSV Di Buat NAMA_KOLOM Besar Semua Tanpa Petik "NAMA_KOLOM"
                writer.WriteLine(builder.ToString().ToUpper());
                foreach (DataRow row in table.Rows) {
                    sep = "";
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

        public int ZipListFileInFolder(string zipFileName, List<string> listFileName = null, string folderPath = null) {
            int totalFileInZip = 0;
            List<string> ls = listFileName ?? ListFileForZip;
            try {
                ZipFile zip = new ZipFile();
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

        public int ZipAllFileInFolder(string zipFileName, string folderPath = null) {
            int totalFileInZip = 0;
            try {
                ZipFile zip = new ZipFile();
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
