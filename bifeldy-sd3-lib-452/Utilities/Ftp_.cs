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

namespace bifeldy_sd3_lib_452.Utilities {

    public interface IFtp {
        Task<FtpClient> CreateFtpConnection(string ipDomainHost, int portNumber, string userName, string password, string remoteWorkDir);
        Task<int> SendFtpFiles(FtpClient ftpConnection, string localDirPath, string fileName = null);
        Task<int> GetFtpFileDir(FtpClient ftpConnection, string localDirFilePath, bool isDirectory = false);
        Task<int> CreateFtpConnectionAndSendFtpFiles(string ipDomainHost, int portNumber, string userName, string password, string remoteWorkDir, string localDirPath, string fileName = null);
        Task<int> CreateFtpConnectionAndGetFtpFileDir(string ipDomainHost, int portNumber, string userName, string password, string remoteWorkDir, string localDirFilePath, bool isDirectory = false);
    }

    public sealed class CFtp : IFtp {

        private readonly ILogger _logger;
        private readonly IBerkas _berkas;

        public CFtp(ILogger logger, IBerkas fileManager) {
            _logger = logger;
            _berkas = fileManager;
        }

        public async Task<FtpClient> CreateFtpConnection(string ipDomainHost, int portNumber, string userName, string password, string remoteWorkDir) {
            FtpClient ftpClient = null;
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
            }
            return ftpClient;
        }

        public async Task<int> SendFtpFiles(FtpClient ftpConnection, string localDirPath, string fileName = null) {
            int totalFileSent = 0;
            DirectoryInfo directoryInfo = new DirectoryInfo(localDirPath);
            FileInfo[] fileInfos = directoryInfo.GetFiles();
            if (fileName != null) {
                fileInfos = directoryInfo.GetFiles().Where(f => f.Name.Contains(fileName)).ToArray();
            }
            foreach (FileInfo fi in fileInfos) {
                string fileSent = "Fail";
                if (ftpConnection.FileExists(fi.Name)) {
                    await ftpConnection.DeleteFileAsync(fi.Name);
                }
                FtpStatus ftpStatus = await ftpConnection.UploadFileAsync(fi.FullName, fi.Name);
                if (ftpStatus == FtpStatus.Success) {
                    totalFileSent++;
                    fileSent = "Ok";
                }
                _logger.WriteLog($"{GetType().Name}Sent{fileSent}", fi.FullName);
            }
            if (ftpConnection.IsConnected) {
                await ftpConnection.DisconnectAsync();
            }
            return totalFileSent;
        }

        public async Task<int> GetFtpFileDir(FtpClient ftpConnection, string localDirFilePath, bool isDirectory = false) {
            int totalFileGet = 0;
            string saveDownloadTo = Path.Combine(_berkas.DownloadFolderPath, localDirFilePath);
            if (isDirectory) {
                List<FtpResult> ftpResult = await ftpConnection.DownloadDirectoryAsync(saveDownloadTo, localDirFilePath, FtpFolderSyncMode.Update, FtpLocalExists.Overwrite);
                foreach (FtpResult fr in ftpResult) {
                    string fileGet = "Fail";
                    if (fr.IsSuccess) {
                        totalFileGet++;
                        fileGet = "Ok";
                    }
                    _logger.WriteLog($"{GetType().Name}Get{fileGet}", fr.LocalPath);
                }
            }
            else {
                string fileGet = "Fail";
                FtpStatus ftpStatus = await ftpConnection.DownloadFileAsync(_berkas.DownloadFolderPath, localDirFilePath, FtpLocalExists.Overwrite);
                if (ftpStatus == FtpStatus.Success) {
                    fileGet = "Ok";
                    totalFileGet = 1;
                }
                _logger.WriteLog($"{GetType().Name}Get{fileGet}", saveDownloadTo);
            }
            if (ftpConnection.IsConnected) {
                await ftpConnection.DisconnectAsync();
            }
            return totalFileGet;
        }

        public async Task<int> CreateFtpConnectionAndSendFtpFiles(string ipDomainHost, int portNumber, string userName, string password, string remoteWorkDir, string localDirPath, string fileName = null) {
            FtpClient ftpClient = await CreateFtpConnection(ipDomainHost, portNumber, userName, password, remoteWorkDir);
            int totalFileSent = await SendFtpFiles(ftpClient, localDirPath, fileName);
            return totalFileSent;
        }

        public async Task<int> CreateFtpConnectionAndGetFtpFileDir(string ipDomainHost, int portNumber, string userName, string password, string remoteWorkDir, string localDirFilePath, bool isDirectory = false) {
            FtpClient ftpClient = await CreateFtpConnection(ipDomainHost, portNumber, userName, password, remoteWorkDir);
            int totalFileGet = await GetFtpFileDir(ftpClient, localDirFilePath, isDirectory);
            return totalFileGet;
        }

    }

}
