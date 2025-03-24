/**
 * 
 * Author       :: Basilius Bias Astho Christyono
 * Phone        :: (+62) 889 236 6466
 * 
 * Department   :: IT SD 03
 * Mail         :: bias@indomaret.co.id
 * 
 * Catatan      :: Kurir FTP
 *              :: Harap Didaftarkan Ke DI Container
 * 
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading.Tasks;

using FluentFTP;

using bifeldy_sd3_lib_452.Models;

namespace bifeldy_sd3_lib_452.Utilities {

    public interface IFtp {
        Task<FtpClient> CreateFtpConnection(string ipDomainHost, int portNumber, string userName, string password, string remoteWorkDir);
        Task RenameFtpFiles(FtpClient ftpConnection, string originalFileName, string newFileName, bool deleteNewFileIfExists = false);
        Task<CFtpResultInfo> SendFtpFiles(FtpClient ftpConnection, string localDirPath, string fileName = null, Action<double> progress = null, string renameFileUploadName = null);
        Task<CFtpResultInfo> GetFtpFileDir(FtpClient ftpConnection, string localDirFilePath, string fileName = null, Action<double> progress = null);
        Task CreateFtpConnectionAndRenameFtpFiles(string ipDomainHost, int portNumber, string userName, string password, string remoteWorkDir, string originalFileName, string newFileName, bool deleteNewFileIfExists = false);
        Task<CFtpResultInfo> CreateFtpConnectionAndSendFtpFiles(string ipDomainHost, int portNumber, string userName, string password, string remoteWorkDir, string localDirPath, string fileName = null, Action<double> progress = null, string renameFileUploadName = null);
        Task<CFtpResultInfo> CreateFtpConnectionAndGetFtpFileDir(string ipDomainHost, int portNumber, string userName, string password, string remoteWorkDir, string localDirFilePath, string fileName = null, Action<double> progress = null);
    }

    public sealed class CFtp : IFtp {

        private readonly IApplication _app;
        private readonly ILogger _logger;
        private readonly IBerkas _berkas;

        public CFtp(IApplication app, ILogger logger, IBerkas berkas) {
            this._app = app;
            this._logger = logger;
            this._berkas = berkas;
        }

        public async Task<FtpClient> CreateFtpConnection(string ipDomainHost, int portNumber, string userName, string password, string remoteWorkDir) {
            FtpClient ftpClient;
            try {
                ftpClient = new FtpClient {
                    Host = ipDomainHost,
                    Port = portNumber,
                    Credentials = new NetworkCredential(userName, password),
                    DataConnectionType = FtpDataConnectionType.PASV,
                    DownloadDataType = FtpDataType.Binary,
                    UploadDataType = FtpDataType.Binary
                };
                await ftpClient.ConnectAsync();
                await ftpClient.SetWorkingDirectoryAsync(remoteWorkDir);
            }
            catch (Exception ex) {
                this._logger.WriteError(ex, 3);
                throw ex;
            }

            return ftpClient;
        }

        public async Task RenameFtpFiles(FtpClient ftpConnection, string originalFileName, string newFileName, bool deleteNewFileIfExists = false) {
            if (this._app.DebugMode) {
                originalFileName = $"_SIMULASI__{originalFileName}";
            }

            if (this._app.DebugMode) {
                newFileName = $"_SIMULASI__{newFileName}";
            }

            if (!ftpConnection.FileExists(originalFileName)) {
                throw new Exception($"File {originalFileName} Target Tidak DItemukan!");
            }

            if (ftpConnection.FileExists(newFileName) && deleteNewFileIfExists) {
                await ftpConnection.DeleteFileAsync(newFileName);
            }

            await ftpConnection.RenameAsync(originalFileName, newFileName);

            this._logger.WriteInfo($"{this.GetType().Name}Rename", $"{originalFileName} => {newFileName}");

            if (ftpConnection.IsConnected) {
                await ftpConnection.DisconnectAsync();
            }
        }

        public async Task<CFtpResultInfo> SendFtpFiles(FtpClient ftpConnection, string localDirPath, string fileName = null, Action<double> progress = null, string renameFileUploadName = null) {
            var ftpResultInfo = new CFtpResultInfo();
            var directoryInfo = new DirectoryInfo(localDirPath);
            FileInfo[] fileInfos = directoryInfo.GetFiles();
            if (fileName != null) {
                fileInfos = fileInfos.Where(f => f.Name.Contains(fileName)).ToArray();
            }

            string cwd = await ftpConnection.GetWorkingDirectoryAsync();
            foreach (FileInfo fi in fileInfos) {
                string fileSent = "Fail";

                string fn = renameFileUploadName ?? fi.Name;
                if (this._app.DebugMode) {
                    fn = $"_SIMULASI__{fn}";
                }

                if (ftpConnection.FileExists(fn)) {
                    await ftpConnection.DeleteFileAsync(fn);
                }

                IProgress<FtpProgress> ftpProgress = new Progress<FtpProgress>(data => {
                    progress?.Invoke(data.Progress);
                    this._logger.WriteInfo($"{this.GetType().Name}Send", $"{fi.Name} {data.Progress} %");
                });

                FtpStatus ftpStatus = await ftpConnection.UploadFileAsync(fi.FullName, fn, progress: ftpProgress);

                var resultSend = new CFtpResultSendGet() {
                    FtpStatusSendGet = ftpStatus == FtpStatus.Success,
                    FileInformation = fi
                };

                if (ftpStatus == FtpStatus.Success) {
                    fileSent = "Ok";
                    ftpResultInfo.Success.Add(resultSend);
                }
                else {
                    ftpResultInfo.Fail.Add(resultSend);
                }

                this._logger.WriteInfo($"{this.GetType().Name}Sent{fileSent}", $"{cwd}/{fn}");
            }

            if (ftpConnection.IsConnected) {
                await ftpConnection.DisconnectAsync();
            }

            return ftpResultInfo;
        }

        public async Task<CFtpResultInfo> GetFtpFileDir(FtpClient ftpConnection, string remoteDirFilePath, string fileName = null, Action<double> progress = null) {
            var ftpResultInfo = new CFtpResultInfo();
            string saveDownloadTo = Path.Combine(this._berkas.DownloadFolderPath, remoteDirFilePath);

            IProgress<FtpProgress> ftpProgress = new Progress<FtpProgress>(data => {
                progress?.Invoke(data.Progress);
                this._logger.WriteInfo($"{this.GetType().Name}Get", $"{remoteDirFilePath} {data.Progress} %");
            });

            if (string.IsNullOrEmpty(fileName)) {
                List<FtpResult> ftpResult = await ftpConnection.DownloadDirectoryAsync(saveDownloadTo, remoteDirFilePath, FtpFolderSyncMode.Update, FtpLocalExists.Overwrite, progress: ftpProgress);

                foreach (FtpResult fr in ftpResult) {
                    string fileGet = "Fail";
                    var resultGet = new CFtpResultSendGet() {
                        FtpStatusSendGet = fr.IsSuccess,
                        FileInformation = new FileInfo(Path.Combine(saveDownloadTo, fr.Name))
                    };

                    if (fr.IsSuccess) {
                        fileGet = "Ok";
                        ftpResultInfo.Success.Add(resultGet);
                    }
                    else {
                        ftpResultInfo.Fail.Add(resultGet);
                    }

                    this._logger.WriteInfo($"{this.GetType().Name}Get{fileGet}", fr.LocalPath);
                }
            }
            else {
                string fileGet = "Fail";

                FtpStatus ftpStatus = await ftpConnection.DownloadFileAsync(this._berkas.DownloadFolderPath, fileName, FtpLocalExists.Overwrite, progress: ftpProgress);

                var resultGet = new CFtpResultSendGet() {
                    FtpStatusSendGet = ftpStatus == FtpStatus.Success,
                    FileInformation = new FileInfo(saveDownloadTo)
                };

                if (ftpStatus == FtpStatus.Success) {
                    fileGet = "Ok";
                    ftpResultInfo.Success.Add(resultGet);
                }
                else {
                    ftpResultInfo.Fail.Add(resultGet);
                }

                this._logger.WriteInfo($"{this.GetType().Name}Get{fileGet}", saveDownloadTo);
            }

            if (ftpConnection.IsConnected) {
                await ftpConnection.DisconnectAsync();
            }

            return ftpResultInfo;
        }

        public async Task CreateFtpConnectionAndRenameFtpFiles(string ipDomainHost, int portNumber, string userName, string password, string remoteWorkDir, string originalFileName, string newFileName, bool deleteNewFileIfExists = false) {
            FtpClient ftpClient = await this.CreateFtpConnection(ipDomainHost, portNumber, userName, password, remoteWorkDir);
            await this.RenameFtpFiles(ftpClient, originalFileName, newFileName, deleteNewFileIfExists);
        }

        public async Task<CFtpResultInfo> CreateFtpConnectionAndSendFtpFiles(string ipDomainHost, int portNumber, string userName, string password, string remoteWorkDir, string localDirPath, string fileName = null, Action<double> progress = null, string renameFileUploadName = null) {
            FtpClient ftpClient = await this.CreateFtpConnection(ipDomainHost, portNumber, userName, password, remoteWorkDir);
            return await this.SendFtpFiles(ftpClient, localDirPath, fileName, progress, renameFileUploadName);
        }

        public async Task<CFtpResultInfo> CreateFtpConnectionAndGetFtpFileDir(string ipDomainHost, int portNumber, string userName, string password, string remoteWorkDir, string localDirFilePath, string fileName = null, Action<double> progress = null) {
            FtpClient ftpClient = await this.CreateFtpConnection(ipDomainHost, portNumber, userName, password, remoteWorkDir);
            return await this.GetFtpFileDir(ftpClient, localDirFilePath, fileName, progress);
        }

    }

}
