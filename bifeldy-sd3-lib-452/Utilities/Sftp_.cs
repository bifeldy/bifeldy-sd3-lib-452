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
using System.Collections;

using Tamir.SharpSsh;

namespace bifeldy_sd3_lib_452.Utilities {

    public interface ISftp {
        bool GetFile(string hostname, int port, string username, string password, string remotePath, string localFile);
        bool PutFile(string hostname, int port, string username, string password, string localFile, string remotePath);
        ArrayList GetDirectoryList(string hostname, int port, string username, string password, string remotePath);
    }

    public sealed class CSftp : ISftp {

        private readonly ILogger _logger;

        public CSftp(ILogger logger) {
            _logger = logger;
        }

        public bool GetFile(string hostname, int port, string username, string password, string remotePath, string localFile) {
            try {
                Sftp Transfer = new Sftp(hostname, username, password);
                Transfer.Connect(port);
                Transfer.Get(remotePath, localFile);
                Transfer.Close();
                return true;
            }
            catch (Exception ex) {
                _logger.WriteError(ex, 3);
                return false;
            }
        }

        public bool PutFile(string hostname, int port, string username, string password, string localFile, string remotePath) {
            try {
                Sftp Transfer = new Sftp(hostname, username, password);
                Transfer.Connect(port);
                Transfer.Put(localFile, remotePath);
                Transfer.Close();
                _logger.WriteInfo($"{GetType().Name}SentOk", remotePath);
                return true;
            }
            catch (Exception ex) {
                _logger.WriteInfo($"{GetType().Name}SentFail", remotePath);
                _logger.WriteError(ex, 3);
                return false;
            }
        }

        public ArrayList GetDirectoryList(string hostname, int port, string username, string password, string remotePath) {
            ArrayList response = new ArrayList();
            try {
                Sftp Transfer = new Sftp(hostname, username, password);
                Transfer.Connect(port);
                response = Transfer.GetFileList(remotePath);
                Transfer.Close();
            }
            catch (Exception ex) {
                _logger.WriteError(ex, 3);
            }
            return response;
        }

    }

}
