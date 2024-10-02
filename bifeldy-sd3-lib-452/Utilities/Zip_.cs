/**
 * 
 * Author       :: Basilius Bias Astho Christyono
 * Phone        :: (+62) 889 236 6466
 * 
 * Department   :: IT SD 03
 * Mail         :: bias@indomaret.co.id
 * 
 * Catatan      :: CSV Files Manager
 *              :: Harap Didaftarkan Ke DI Container
 * 
 */

using System;
using System.Collections.Generic;
using System.IO;

using ICSharpCode.SharpZipLib.Zip;

namespace bifeldy_sd3_lib_452.Utilities {

    public interface IZip {
        string ZipFolderPath { get; }
        List<string> ListFileForZip { get; }
        int ZipListFileInFolder(string zipFileName, string folderPath, List<string> listFileName = null, string password = null, string outputPath = null);
        int ZipAllFileInFolder(string zipFileName, string folderPath, string password = null, string outputPath = null);
    }

    public sealed class CZip : IZip {

        private readonly IApplication _app;
        private readonly ILogger _logger;
        private readonly IConfig _config;

        public string ZipFolderPath { get; }

        public List<string> ListFileForZip { get; } = new List<string>();

        public CZip(IApplication app, ILogger logger, IConfig config) {
            this._app = app;
            this._logger = logger;
            this._config = config;

            this.ZipFolderPath = this._config.Get<string>("ZipFolderPath", Path.Combine(this._app.AppLocation, "_data", "Zip_Files"));
            if (!Directory.Exists(this.ZipFolderPath)) {
                Directory.CreateDirectory(this.ZipFolderPath);
            }
        }

        public int ZipListFileInFolder(string zipFileName, string folderPath, List<string> listFileName = null, string password = null, string outputPath = null) {
            int totalFileInZip = 0;
            List<string> ls = listFileName ?? this.ListFileForZip;
            string path = Path.Combine(outputPath ?? this.ZipFolderPath, zipFileName);
            try {
                using (var zip = new ZipOutputStream(File.Create(path))) {
                    zip.SetLevel(9); // 0 - store only to 9 - means best compression
                    if (!string.IsNullOrEmpty(password)) {
                        zip.Password = password;
                    }

                    byte[] buffer = new byte[4096];
                    foreach (string targetFileName in ls) {
                        string filePath = Path.Combine(folderPath, targetFileName);
                        var zipEntry = new ZipEntry(filePath) {
                            DateTime = DateTime.Now
                        };
                        zip.PutNextEntry(zipEntry);

                        using (FileStream fs = File.OpenRead(filePath)) {
                            int sourceBytes;
                            do {
                                sourceBytes = fs.Read(buffer, 0, buffer.Length);
                                zip.Write(buffer, 0, sourceBytes);
                            }
                            while (sourceBytes > 0);
                        }

                        this._logger.WriteInfo($"{this.GetType().Name}ZipAdd{(zipEntry == null ? "Fail" : "Ok")}", filePath);
                    }

                    zip.Finish();
                    zip.Close();
                    this._logger.WriteInfo($"{this.GetType().Name}ZipSave", path);
                }
            }
            catch (Exception ex) {
                this._logger.WriteError(ex.Message);
            }
            finally {
                if (listFileName == null) {
                    ls.Clear();
                }
            }

            return totalFileInZip;
        }

        public int ZipAllFileInFolder(string zipFileName, string folderPath, string password = null, string outputPath = null) {
            int totalFileInZip = 0;
            string path = Path.Combine(outputPath ?? this.ZipFolderPath, zipFileName);
            try {
                using (var zip = new ZipOutputStream(File.Create(path))) {
                    zip.SetLevel(9); // 0 - store only to 9 - means best compression
                    if (!string.IsNullOrEmpty(password)) {
                        zip.Password = password;
                    }

                    byte[] buffer = new byte[4096];
                    var directoryInfo = new DirectoryInfo(folderPath);
                    FileInfo[] fileInfos = directoryInfo.GetFiles();
                    foreach (FileInfo fileInfo in fileInfos) {
                        var zipEntry = new ZipEntry(fileInfo.FullName) {
                            DateTime = DateTime.Now
                        };
                        zip.PutNextEntry(zipEntry);
                        totalFileInZip++;

                        using (FileStream fs = File.OpenRead(fileInfo.FullName)) {
                            int sourceBytes;
                            do {
                                sourceBytes = fs.Read(buffer, 0, buffer.Length);
                                zip.Write(buffer, 0, sourceBytes);
                            }
                            while (sourceBytes > 0);
                        }

                        this._logger.WriteInfo($"{this.GetType().Name}ZipAdd{(zipEntry == null ? "Fail" : "Ok")}", fileInfo.FullName);
                    }

                    zip.Finish();
                    zip.Close();
                    this._logger.WriteInfo($"{this.GetType().Name}ZipSave", path);
                }
            }
            catch (Exception ex) {
                this._logger.WriteError(ex.Message);
            }

            return totalFileInZip;
        }

    }

}
