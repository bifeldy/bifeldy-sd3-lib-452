/**
 * 
 * Author       :: Basilius Bias Astho Christyono
 * Phone        :: (+62) 889 236 6466
 * 
 * Department   :: IT SD 03
 * Mail         :: bias@indomaret.co.id
 * 
 * Catatan      :: Encrypt / Decrypt String
 *              :: Harap Didaftarkan Ke DI Container
 * 
 */

using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Security.Cryptography;
using System.Runtime.InteropServices;

using Ionic.Crc;

using bifeldy_sd3_lib_452.Extensions;

namespace bifeldy_sd3_lib_452.Utilities {

    public interface IChiper {
        string EncryptText(string plainText, string passPhrase = null);
        string DecryptText(string cipherText, string passPhrase = null, Encoding encoding = null);
        string CalculateMD5File(string filePath);
        string CalculateCRC32File(string filePath);
        string CalculateSHA1File(string filePath);
        string GetMime(string filePath);
        string HashByte(byte[] data);
        string HashText(string textMessage);
    }

    public sealed class CChiper : IChiper {

        private string AppName { get; }

        // This constant is used to determine the keysize of the encryption algorithm in bits.
        // We divide this by 8 within the code below to get the equivalent number of bytes.
        private const int Keysize = 128;
        private const int Blocksize = 128;

        // This constant determines the number of iterations for the password bytes generation function.
        private const int DerivationIterations = 1000;

        public CChiper() {
            // Tidak bisa pakai Dependency Injection :: IApplication -> IConfig -> IChiper (Circular DI)
            string appName = Process.GetCurrentProcess().MainModule.ModuleName.ToUpper();
            this.AppName = appName.Substring(0, appName.LastIndexOf(".EXE"));
        }

        private byte[] Generate128BitsOfRandomEntropy() {
            byte[] randomBytes = new byte[16]; // 16 Bytes will give us 128 bits.
            using (var rngCsp = RandomNumberGenerator.Create()) {
                // Fill the array with cryptographically secure random bytes.
                rngCsp.GetBytes(randomBytes);
            }

            return randomBytes;
        }

        public string EncryptText(string plainText, string passPhrase = null) {
            if (string.IsNullOrEmpty(passPhrase) || passPhrase?.Length < 8) {
                passPhrase = this.HashText(this.AppName);
            }
            // Salt and IV is randomly generated each time, but is preprended to encrypted cipher text
            // so that the same Salt and IV values can be used when decrypting.  
            byte[] saltStringBytes = this.Generate128BitsOfRandomEntropy();
            byte[] ivStringBytes = this.Generate128BitsOfRandomEntropy();
            byte[] plainTextBytes = Encoding.UTF8.GetBytes(plainText);
            using (var password = new Rfc2898DeriveBytes(passPhrase, saltStringBytes, DerivationIterations)) {
                byte[] keyBytes = password.GetBytes(Keysize / 8);
                using (var symmetricKey = Aes.Create()) {
                    symmetricKey.BlockSize = Blocksize;
                    symmetricKey.Mode = CipherMode.CBC;
                    symmetricKey.Padding = PaddingMode.PKCS7;
                    using (ICryptoTransform encryptor = symmetricKey.CreateEncryptor(keyBytes, ivStringBytes)) {
                        using (var memoryStream = new MemoryStream()) {
                            using (var cryptoStream = new CryptoStream(memoryStream, encryptor, CryptoStreamMode.Write)) {
                                cryptoStream.Write(plainTextBytes, 0, plainTextBytes.Length);
                                cryptoStream.FlushFinalBlock();
                                // Create the final bytes as a concatenation of the random salt bytes, the random iv bytes and the cipher bytes.
                                byte[] cipherTextBytes = saltStringBytes;
                                cipherTextBytes = cipherTextBytes.Concat(ivStringBytes).ToArray();
                                cipherTextBytes = cipherTextBytes.Concat(memoryStream.ToArray()).ToArray();
                                return Convert.ToBase64String(cipherTextBytes);
                            }
                        }
                    }
                }
            }
        }

        public string DecryptText(string cipherText, string passPhrase = null, Encoding encoding = null) {
            if (string.IsNullOrEmpty(passPhrase) || passPhrase?.Length < 8) {
                passPhrase = this.HashText(this.AppName);
            }
            // Get the complete stream of bytes that represent:
            // [32 bytes of Salt] + [32 bytes of IV] + [n bytes of CipherText]
            byte[] cipherTextBytesWithSaltAndIv = Convert.FromBase64String(cipherText);
            // Get the saltbytes by extracting the first 32 bytes from the supplied cipherText bytes.
            byte[] saltStringBytes = cipherTextBytesWithSaltAndIv.Take(Keysize / 8).ToArray();
            // Get the IV bytes by extracting the next 32 bytes from the supplied cipherText bytes.
            byte[] ivStringBytes = cipherTextBytesWithSaltAndIv.Skip(Keysize / 8).Take(Keysize / 8).ToArray();
            // Get the actual cipher text bytes by removing the first 64 bytes from the cipherText string.
            byte[] cipherTextBytes = cipherTextBytesWithSaltAndIv.Skip(Keysize / 8 * 2).Take(cipherTextBytesWithSaltAndIv.Length - (Keysize / 8 * 2)).ToArray();
            using (var password = new Rfc2898DeriveBytes(passPhrase, saltStringBytes, DerivationIterations)) {
                byte[] keyBytes = password.GetBytes(Keysize / 8);
                using (var symmetricKey = Aes.Create()) {
                    symmetricKey.BlockSize = Blocksize;
                    symmetricKey.Mode = CipherMode.CBC;
                    symmetricKey.Padding = PaddingMode.PKCS7;
                    using (ICryptoTransform decryptor = symmetricKey.CreateDecryptor(keyBytes, ivStringBytes)) {
                        using (var memoryStream = new MemoryStream(cipherTextBytes)) {
                            using (var cryptoStream = new CryptoStream(memoryStream, decryptor, CryptoStreamMode.Read)) {
                                using (var streamReader = new StreamReader(cryptoStream, encoding ?? Encoding.UTF8)) {
                                    return streamReader.ReadToEnd();
                                }
                            }
                        }
                    }
                }
            }
        }

        public string CalculateMD5File(string filePath) {
            using (var md5 = MD5.Create()) {
                using (FileStream stream = File.OpenRead(filePath)) {
                    return md5.ComputeHash(stream).ToStringHex();
                }
            }
        }

        public string CalculateCRC32File(string filePath) {
            using (FileStream stream = File.OpenRead(filePath)) {
                return new CRC32().GetCrc32(stream).ToString("x");
            }
        }

        public string CalculateSHA1File(string filePath) {
            using (var sha1 = SHA1.Create()) {
                using (FileStream stream = File.OpenRead(filePath)) {
                    return sha1.ComputeHash(stream).ToStringHex();
                }
            }
        }

        [DllImport("urlmon.dll", CharSet = CharSet.Unicode, ExactSpelling = true, SetLastError = false)]
        private static extern int FindMimeFromData(
            IntPtr pBC,
            [MarshalAs(UnmanagedType.LPWStr)] string pwzUrl,
            [MarshalAs(UnmanagedType.LPArray, ArraySubType = UnmanagedType.I1, SizeParamIndex = 3)] byte[] pBuffer,
            int cbSize,
            [MarshalAs(UnmanagedType.LPWStr)] string pwzMimeProposed,
            int dwMimeFlags,
            out IntPtr ppwzMimeOut,
            int dwReserved
        );

        public string GetMime(string filePath) {
            if (!File.Exists(filePath)) {
                throw new FileNotFoundException(filePath + " Not Found");
            }

            const int maxContent = 256;
            byte[] buffer = new byte[maxContent];
            using (var fs = new FileStream(filePath, FileMode.Open)) {
                if (fs.Length >= maxContent) {
                    _ = fs.Read(buffer, 0, maxContent);
                }
                else {
                    _ = fs.Read(buffer, 0, (int)fs.Length);
                }
            }

            IntPtr mimeTypePtr = IntPtr.Zero;
            try {
                int result = FindMimeFromData(IntPtr.Zero, null, buffer, maxContent, null, 0, out mimeTypePtr, 0);
                if (result != 0) {
                    Marshal.FreeCoTaskMem(mimeTypePtr);
                    throw Marshal.GetExceptionForHR(result);
                }

                string mime = Marshal.PtrToStringUni(mimeTypePtr);
                Marshal.FreeCoTaskMem(mimeTypePtr);
                return mime;
            }
            catch {
                if (mimeTypePtr != IntPtr.Zero) {
                    Marshal.FreeCoTaskMem(mimeTypePtr);
                }

                return "application/octet-stream";
            }
        }

        public string HashByte(byte[] data) {
            using (var sha1 = SHA1.Create()) {
                byte[] hash = sha1.ComputeHash(data);
                return hash.ToStringHex();
            }
        }

        public string HashText(string textMessage) {
            byte[] data = Encoding.UTF8.GetBytes(textMessage);
            return this.HashByte(data);
        }

    }

}
