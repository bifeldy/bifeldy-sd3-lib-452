/**
 * 
 * Author       :: Basilius Bias Astho Christyono
 * Phone        :: (+62) 889 236 6466
 * 
 * Department   :: IT SD 03
 * Mail         :: bias@indomaret.co.id
 * 
 * Catatan      :: Secure SSH
 *              :: Harap Didaftarkan Ke DI Container
 * 
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

using Renci.SshNet;
using Renci.SshNet.Sftp;

namespace bifeldy_sd3_lib_452.Utilities {

    public interface ISftp {
        bool GetFile(string hostname, int port, string username, string password, string remotePath, string localFile, Action<double> progress = null);
        bool PutFile(string hostname, int port, string username, string password, string localFile, string remotePath, Action<double> progress = null);
        string[] GetDirectoryList(string hostname, int port, string username, string password, string remotePath);
    }

    public sealed class CSftp : ISftp {

        private readonly ILogger _logger;

        public CSftp(ILogger logger) {
            this._logger = logger;
        }

        public bool GetFile(string hostname, int port, string username, string password, string remotePath, string localFile, Action<double> progress = null) {
            try {
                using (var sftp = new SftpClient(hostname, port, username, password)) {
                    sftp.Connect();
                    using (var fs = new FileStream(localFile, FileMode.OpenOrCreate)) {
                        sftp.DownloadFile(remotePath, fs, downloaded => {
                            double percentage = (double) downloaded / fs.Length * 100;
                            progress?.Invoke(percentage);
                            this._logger.WriteInfo($"{this.GetType().Name}Get", $"{fs.Name} {percentage} %");
                        });
                    }

                    sftp.Disconnect();
                }

                return true;
            }
            catch (Exception ex) {
                this._logger.WriteError(ex, 3);
                return false;
            }
        }

        public bool PutFile(string hostname, int port, string username, string password, string localFile, string remotePath, Action<double> progress = null) {
            try {
                using (var sftp = new SftpClient(hostname, port, username, password)) {
                    sftp.Connect();
                    using (var fs = new FileStream(localFile, FileMode.Open)) {
                        sftp.UploadFile(fs, remotePath, uploaded => {
                            double percentage = (double) uploaded / fs.Length * 100;
                            progress?.Invoke(percentage);
                            this._logger.WriteInfo($"{this.GetType().Name}Put", $"{fs.Name} {percentage} %");
                        });
                    }

                    sftp.Disconnect();
                }

                return true;
            }
            catch (Exception ex) {
                this._logger.WriteError(ex, 3);
                return false;
            }
        }

        public string[] GetDirectoryList(string hostname, int port, string username, string password, string remotePath) {
            var response = new List<SftpFile>();
            try {
                using (var sftp = new SftpClient(hostname, port, username, password)) {
                    sftp.Connect();
                    response = (List<SftpFile>) sftp.ListDirectory(remotePath);
                    sftp.Disconnect();
                }
            }
            catch (Exception ex) {
                this._logger.WriteError(ex, 3);
            }

            return response.Select(r => r.FullName).ToArray();
        }

    }

}
