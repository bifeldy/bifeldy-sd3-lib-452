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
using System.IO;

namespace bifeldy_sd3_lib_452.Utilities {

    public interface IBerkas {
        int MaxOldRetentionDay { get; set; }
        string BackupFolderPath { get; }
        string DownloadFolderPath { get; }
        void DeleteSingleFileInFolder(string fileName, string folderPath);
        void DeleteOldFilesInFolder(string folderPath, int maxOldDays, bool isInRecursive = false);
        void CleanUp();
        void CopyAllFilesAndDirectories(DirectoryInfo source, DirectoryInfo target, bool isInRecursive = false);
        void BackupAllFilesInFolder(string folderPath);
    }

    public sealed class CBerkas : IBerkas {

        private readonly IApplication _app;
        private readonly ILogger _logger;
        private readonly IConfig _config;
        private readonly ICsv _csv;
        private readonly IZip _zip;

        public int MaxOldRetentionDay { get; set; }

        public string BackupFolderPath { get; }
        public string DownloadFolderPath { get; }

        public CBerkas(IApplication app, ILogger logger, IConfig config, ICsv csv, IZip zip) {
            _app = app;
            _logger = logger;
            _config = config;
            _csv = csv;
            _zip = zip;

            MaxOldRetentionDay = _config.Get<int>("MaxOldRetentionDay", int.Parse(_app.GetConfig("max_old_retention_day")));

            BackupFolderPath = _config.Get<string>("BackupFolderPath", Path.Combine(_app.AppLocation, "_data", "Backup_Files"));
            if (!Directory.Exists(BackupFolderPath)) {
                Directory.CreateDirectory(BackupFolderPath);
            }

            DownloadFolderPath = _config.Get<string>("DownloadFolderPath", Path.Combine(_app.AppLocation, "_data", "Download_Files"));
            if (!Directory.Exists(DownloadFolderPath)) {
                Directory.CreateDirectory(DownloadFolderPath);
            }
        }

        public void DeleteSingleFileInFolder(string fileName, string folderPath) {
            try {
                FileInfo fi = new FileInfo(Path.Combine(folderPath, fileName));
                if (fi.Exists) {
                    fi.Delete();
                }
            }
            catch (Exception ex) {
                _logger.WriteError(ex.Message);
            }
        }

        public void DeleteOldFilesInFolder(string folderPath, int maxOldDays, bool isInRecursive = false) {
            try {
                if (Directory.Exists(folderPath)) {
                    DirectoryInfo di = new DirectoryInfo(folderPath);
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

        public void CleanUp() {
            DeleteOldFilesInFolder(_logger.LogInfoFolderPath, MaxOldRetentionDay);
            DeleteOldFilesInFolder(_logger.LogErrorFolderPath, MaxOldRetentionDay);
            DeleteOldFilesInFolder(BackupFolderPath, MaxOldRetentionDay);
            DeleteOldFilesInFolder(DownloadFolderPath, MaxOldRetentionDay);
            DeleteOldFilesInFolder(_csv.CsvFolderPath, MaxOldRetentionDay);
            DeleteOldFilesInFolder(_zip.ZipFolderPath, MaxOldRetentionDay);
            _zip.ListFileForZip.Clear();
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

        public void BackupAllFilesInFolder(string folderPath) {
            DirectoryInfo diSource = new DirectoryInfo(folderPath);
            DirectoryInfo diTarget = new DirectoryInfo(BackupFolderPath);
            CopyAllFilesAndDirectories(diSource, diTarget);
        }

    }

}
