/**
 * 
 * Author       :: Basilius Bias Astho Christyono
 * Mail         :: bias@indomaret.co.id
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
        Task<CFtpResultInfo> GetFtpFileDir(FtpClient ftpConnection, string localDirFilePath, bool isDirectory = false);
        Task<CFtpResultInfo> CreateFtpConnectionAndSendFtpFiles(string ipDomainHost, int portNumber, string userName, string password, string remoteWorkDir, string localDirPath, string fileName = null);
        Task<CFtpResultInfo> CreateFtpConnectionAndGetFtpFileDir(string ipDomainHost, int portNumber, string userName, string password, string remoteWorkDir, string localDirFilePath, bool isDirectory = false);
    }

    public sealed class CFtp : IFtp {

        private readonly ILogger _logger;
        private readonly IBerkas _berkas;

        public CFtp(ILogger logger, IBerkas fileManager) {
            _logger = logger;
            _berkas = fileManager;
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
                string fn = fi.Name;
                #if DEBUG
                    fn = $"_SIMULASI__{fi.Name}";
                #endif
                if (ftpConnection.FileExists(fn)) {
                    await ftpConnection.DeleteFileAsync(fn);
                }
                FtpStatus ftpStatus = await ftpConnection.UploadFileAsync(fi.FullName, fn);
                CFtpResultSendGet resultSend = new CFtpResultSendGet() {
                    FtpStatusSendGet = ftpStatus,
                    FileInformation = fi
                };
                if (ftpStatus == FtpStatus.Success) {
                    fileSent = "Ok";
                    ftpResultInfo.Success.Add(resultSend);
                }
                else {
                    ftpResultInfo.Fail.Add(resultSend);
                }
                _logger.WriteInfo($"{GetType().Name}Sent{fileSent}", $"{cwd}{fn}");
            }
            if (ftpConnection.IsConnected) {
                await ftpConnection.DisconnectAsync();
            }
            return ftpResultInfo;
        }

        public async Task<CFtpResultInfo> GetFtpFileDir(FtpClient ftpConnection, string localDirFilePath, bool isDirectory = false) {
            CFtpResultInfo ftpResultInfo = new CFtpResultInfo();
            string saveDownloadTo = Path.Combine(_berkas.DownloadFolderPath, localDirFilePath);
            if (isDirectory) {
                List<FtpResult> ftpResult = await ftpConnection.DownloadDirectoryAsync(saveDownloadTo, localDirFilePath, FtpFolderSyncMode.Update, FtpLocalExists.Overwrite);
                foreach (FtpResult fr in ftpResult) {
                    string fileGet = "Fail";
                    CFtpResultSendGet resultGet = new CFtpResultSendGet() {
                        FtpStatusSendGet = fr.IsSuccess ? FtpStatus.Success : FtpStatus.Failed,
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
                FtpStatus ftpStatus = await ftpConnection.DownloadFileAsync(_berkas.DownloadFolderPath, localDirFilePath, FtpLocalExists.Overwrite);
                CFtpResultSendGet resultGet = new CFtpResultSendGet() {
                    FtpStatusSendGet = ftpStatus,
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

        public async Task<CFtpResultInfo> CreateFtpConnectionAndGetFtpFileDir(string ipDomainHost, int portNumber, string userName, string password, string remoteWorkDir, string localDirFilePath, bool isDirectory = false) {
            FtpClient ftpClient = await CreateFtpConnection(ipDomainHost, portNumber, userName, password, remoteWorkDir);
            return await GetFtpFileDir(ftpClient, localDirFilePath, isDirectory);
        }

    }

}
