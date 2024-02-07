﻿/**
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

using Ionic.Zip;
using Ionic.Zlib;

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
            _app = app;
            _logger = logger;
            _config = config;

            ZipFolderPath = _config.Get<string>("ZipFolderPath", Path.Combine(_app.AppLocation, "_data", "Zip_Files"));
            if (!Directory.Exists(ZipFolderPath)) {
                Directory.CreateDirectory(ZipFolderPath);
            }
        }

        public int ZipListFileInFolder(string zipFileName, string folderPath, List<string> listFileName = null, string password = null, string outputPath = null) {
            int totalFileInZip = 0;
            List<string> ls = listFileName ?? ListFileForZip;
            string path = Path.Combine(outputPath ?? ZipFolderPath, zipFileName);
            try {
                ZipFile zip = new ZipFile();
                if (!string.IsNullOrEmpty(password)) {
                    zip.Password = password;
                    zip.CompressionLevel = CompressionLevel.BestCompression;
                }
                foreach (string targetFileName in ls) {
                    string filePath = Path.Combine(folderPath, targetFileName);
                    ZipEntry zipEntry = zip.AddFile(filePath, "");
                    if (zipEntry != null) {
                        totalFileInZip++;
                    }
                    _logger.WriteInfo($"{GetType().Name}ZipAdd{(zipEntry == null ? "Fail" : "Ok")}", filePath);
                }
                zip.Save(path);
                _logger.WriteInfo($"{GetType().Name}ZipSave", path);
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

        public int ZipAllFileInFolder(string zipFileName, string folderPath, string password = null, string outputPath = null) {
            int totalFileInZip = 0;
            string path = Path.Combine(outputPath ?? ZipFolderPath, zipFileName);
            try {
                ZipFile zip = new ZipFile();
                if (!string.IsNullOrEmpty(password)) {
                    zip.Password = password;
                    zip.CompressionLevel = CompressionLevel.BestCompression;
                }
                DirectoryInfo directoryInfo = new DirectoryInfo(folderPath);
                FileInfo[] fileInfos = directoryInfo.GetFiles();
                foreach (FileInfo fileInfo in fileInfos) {
                    ZipEntry zipEntry = zip.AddFile(fileInfo.FullName, "");
                    if (zipEntry != null) {
                        totalFileInZip++;
                    }
                    _logger.WriteInfo($"{GetType().Name}ZipAdd{(zipEntry == null ? "Fail" : "Ok")}", fileInfo.FullName);
                }
                zip.Save(path);
                _logger.WriteInfo($"{GetType().Name}ZipSave", path);
            }
            catch (Exception ex) {
                _logger.WriteError(ex.Message);
            }
            return totalFileInZip;
        }

    }

}
