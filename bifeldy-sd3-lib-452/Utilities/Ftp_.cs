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
        Task<CFtpResultInfo> SendFtpFiles(FtpClient ftpConnection, string localDirPath, string fileName = null);
        Task<CFtpResultInfo> GetFtpFileDir(FtpClient ftpConnection, string localDirFilePath, string fileName = null);
        Task<CFtpResultInfo> CreateFtpConnectionAndSendFtpFiles(string ipDomainHost, int portNumber, string userName, string password, string remoteWorkDir, string localDirPath, string fileName = null);
        Task<CFtpResultInfo> CreateFtpConnectionAndGetFtpFileDir(string ipDomainHost, int portNumber, string userName, string password, string remoteWorkDir, string localDirFilePath, string fileName = null);
    }

    public sealed class CFtp : IFtp {

        private readonly IApplication _app;
        private readonly ILogger _logger;
        private readonly IBerkas _berkas;

        public CFtp(IApplication app, ILogger logger, IBerkas berkas) {
            _app = app;
            _logger = logger;
            _berkas = berkas;
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
                _logger.WriteError(ex, 3);
                throw ex;
            }
            return ftpClient;
        }

        public async Task<CFtpResultInfo> SendFtpFiles(FtpClient ftpConnection, string localDirPath, string fileName = null) {
            CFtpResultInfo ftpResultInfo = new CFtpResultInfo();
            DirectoryInfo directoryInfo = new DirectoryInfo(localDirPath);
            FileInfo[] fileInfos = directoryInfo.GetFiles();
            if (fileName != null) {
                fileInfos = fileInfos.Where(f => f.Name.Contains(fileName)).ToArray();
            }
            string cwd = await ftpConnection.GetWorkingDirectoryAsync();
            foreach (FileInfo fi in fileInfos) {
                string fileSent = "Fail";
                string fn = _app.DebugMode ? $"_SIMULASI__{fi.Name}" : fi.Name;
                if (ftpConnection.FileExists(fn)) {
                    await ftpConnection.DeleteFileAsync(fn);
                }
                FtpStatus ftpStatus = await ftpConnection.UploadFileAsync(fi.FullName, fn);
                CFtpResultSendGet resultSend = new CFtpResultSendGet() {
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
                _logger.WriteInfo($"{GetType().Name}Sent{fileSent}", $"{cwd}/{fn}");
            }
            if (ftpConnection.IsConnected) {
                await ftpConnection.DisconnectAsync();
            }
            return ftpResultInfo;
        }

        public async Task<CFtpResultInfo> GetFtpFileDir(FtpClient ftpConnection, string localDirFilePath, string fileName = null) {
            CFtpResultInfo ftpResultInfo = new CFtpResultInfo();
            string saveDownloadTo = Path.Combine(_berkas.DownloadFolderPath, localDirFilePath);
            if (string.IsNullOrEmpty(fileName)) {
                List<FtpResult> ftpResult = await ftpConnection.DownloadDirectoryAsync(saveDownloadTo, localDirFilePath, FtpFolderSyncMode.Update, FtpLocalExists.Overwrite);
                foreach (FtpResult fr in ftpResult) {
                    string fileGet = "Fail";
                    CFtpResultSendGet resultGet = new CFtpResultSendGet() {
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
                    _logger.WriteInfo($"{GetType().Name}Get{fileGet}", fr.LocalPath);
                }
            }
            else {
                string fileGet = "Fail";
                FtpStatus ftpStatus = await ftpConnection.DownloadFileAsync(_berkas.DownloadFolderPath, fileName, FtpLocalExists.Overwrite);
                CFtpResultSendGet resultGet = new CFtpResultSendGet() {
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
                _logger.WriteInfo($"{GetType().Name}Get{fileGet}", saveDownloadTo);
            }
            if (ftpConnection.IsConnected) {
                await ftpConnection.DisconnectAsync();
            }
            return ftpResultInfo;
        }

        public async Task<CFtpResultInfo> CreateFtpConnectionAndSendFtpFiles(string ipDomainHost, int portNumber, string userName, string password, string remoteWorkDir, string localDirPath, string fileName = null) {
            FtpClient ftpClient = await CreateFtpConnection(ipDomainHost, portNumber, userName, password, remoteWorkDir);
            return await SendFtpFiles(ftpClient, localDirPath, fileName);
        }

        public async Task<CFtpResultInfo> CreateFtpConnectionAndGetFtpFileDir(string ipDomainHost, int portNumber, string userName, string password, string remoteWorkDir, string localDirFilePath, string fileName = null) {
            FtpClient ftpClient = await CreateFtpConnection(ipDomainHost, portNumber, userName, password, remoteWorkDir);
            return await GetFtpFileDir(ftpClient, localDirFilePath, fileName);
        }

    }

}
