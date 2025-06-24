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
using System.Globalization;
using System.IO;
using System.Text;

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
        bool CheckSign(FileInfo fileInfo, string signFull, bool isRequired = true, Encoding encoding = null);
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
                _ = Directory.CreateDirectory(this.BackupFolderPath);
            }

            this.DownloadFolderPath = this._config.Get<string>("DownloadFolderPath", Path.Combine(this._app.AppLocation, "_data", "Download_Files"));
            if (!Directory.Exists(this.DownloadFolderPath)) {
                _ = Directory.CreateDirectory(this.DownloadFolderPath);
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
                this._logger.WriteError(ex);
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
                this._logger.WriteError(ex);
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
            _ = Directory.CreateDirectory(target.FullName);
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

        public bool CheckSign(FileInfo fileInfo, string signFull, bool isRequired = true, Encoding encoding = null) {
            if (isRequired && string.IsNullOrEmpty(signFull)) {
                throw new Exception("Tidak Ada Tanda Tangan File");
            }
            else if (!isRequired && string.IsNullOrEmpty(signFull)) {
                return true;
            }

            string[] signSplit = signFull.Split(' ');
            int minFileSize = signSplit.Length;
            if (fileInfo.Length < minFileSize) {
                throw new Exception("Isi Konten File Tidak Sesuai");
            }

            int[] intList = new int[minFileSize];
            for (int i = 0; i < intList.Length; i++) {
                if (signSplit[i] == "??") {
                    intList[i] = -1;
                }
                else {
                    intList[i] = int.Parse(signSplit[i], NumberStyles.HexNumber);
                }
            }

            using (var reader = new BinaryReader(new FileStream(fileInfo.FullName, FileMode.Open), encoding ?? Encoding.UTF8, encoding == null)) {
                byte[] buff = new byte[minFileSize];
                _ = reader.BaseStream.Seek(0, SeekOrigin.Begin);
                _ = reader.Read(buff, 0, buff.Length);
                for (int i = 0; i < intList.Length; i++) {
                    if (intList[i] == -1 || buff[i] == intList[i]) {
                        continue;
                    }

                    return false;
                }
            }

            return true;
        }

    }

}
