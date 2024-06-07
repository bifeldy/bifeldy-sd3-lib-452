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
        void CleanUp(bool clearPendingFileForZip = true);
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
            this._app = app;
            this._logger = logger;
            this._config = config;
            this._csv = csv;
            this._zip = zip;

            this.MaxOldRetentionDay = this._config.Get<int>("MaxOldRetentionDay", int.Parse(this._app.GetConfig("max_old_retention_day")));

            this.BackupFolderPath = this._config.Get<string>("BackupFolderPath", Path.Combine(this._app.AppLocation, "_data", "Backup_Files"));
            if (!Directory.Exists(this.BackupFolderPath)) {
                Directory.CreateDirectory(this.BackupFolderPath);
            }

            this.DownloadFolderPath = this._config.Get<string>("DownloadFolderPath", Path.Combine(this._app.AppLocation, "_data", "Download_Files"));
            if (!Directory.Exists(this.DownloadFolderPath)) {
                Directory.CreateDirectory(this.DownloadFolderPath);
            }
        }

        public void DeleteSingleFileInFolder(string fileName, string folderPath) {
            try {
                var fi = new FileInfo(Path.Combine(folderPath, fileName));
                if (fi.Exists) {
                    fi.Delete();
                }
            }
            catch (Exception ex) {
                this._logger.WriteError(ex.Message);
            }
        }

        public void DeleteOldFilesInFolder(string folderPath, int maxOldDays, bool isInRecursive = false) {
            try {
                if (Directory.Exists(folderPath)) {
                    var di = new DirectoryInfo(folderPath);
                    FileSystemInfo[] fsis = di.GetFileSystemInfos();
                    foreach (FileSystemInfo fsi in fsis) {
                        if (fsi.Attributes == FileAttributes.Directory) {
                            this.DeleteOldFilesInFolder(fsi.FullName, maxOldDays, true);
                        }

                        if (fsi.LastWriteTime <= DateTime.Now.AddDays(-maxOldDays)) {
                            this._logger.WriteInfo($"{this.GetType().Name}DelFileDir", fsi.FullName);
                            fsi.Delete();
                        }
                    }
                }
            }
            catch (Exception ex) {
                this._logger.WriteError(ex.Message);
            }
        }

        public void CleanUp(bool clearPendingFileForZip = true) {
            this.DeleteOldFilesInFolder(this._logger.LogInfoFolderPath, this.MaxOldRetentionDay);
            this.DeleteOldFilesInFolder(this._logger.LogErrorFolderPath, this.MaxOldRetentionDay);
            this.DeleteOldFilesInFolder(this.BackupFolderPath, this.MaxOldRetentionDay);
            this.DeleteOldFilesInFolder(this.DownloadFolderPath, this.MaxOldRetentionDay);
            this.DeleteOldFilesInFolder(this._csv.CsvFolderPath, this.MaxOldRetentionDay);
            this.DeleteOldFilesInFolder(this._zip.ZipFolderPath, this.MaxOldRetentionDay);
            if (clearPendingFileForZip) {
                this._zip.ListFileForZip.Clear();
            }
        }

        public void CopyAllFilesAndDirectories(DirectoryInfo source, DirectoryInfo target, bool isInRecursive = false) {
            Directory.CreateDirectory(target.FullName);
            foreach (FileInfo fi in source.GetFiles()) {
                FileInfo res = fi.CopyTo(Path.Combine(target.FullName, fi.Name), true);
                this._logger.WriteInfo($"{this.GetType().Name}CopyAndReplace", $"{fi.FullName} => {res.FullName}");
            }

            foreach (DirectoryInfo diSourceSubDir in source.GetDirectories()) {
                DirectoryInfo nextTargetSubDir = target.CreateSubdirectory(diSourceSubDir.Name);
                this.CopyAllFilesAndDirectories(diSourceSubDir, nextTargetSubDir, true);
            }
        }

        public void BackupAllFilesInFolder(string folderPath) {
            var diSource = new DirectoryInfo(folderPath);
            var diTarget = new DirectoryInfo(this.BackupFolderPath);
            this.CopyAllFilesAndDirectories(diSource, diTarget);
        }

    }

}
