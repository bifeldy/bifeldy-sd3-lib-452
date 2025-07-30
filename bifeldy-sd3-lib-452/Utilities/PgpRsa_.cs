/**
 * 
 * Author       :: Basilius Bias Astho Christyono
 * Phone        :: (+62) 889 236 6466
 * 
 * Department   :: IT SD 03
 * Mail         :: bias@indomaret.co.id
 * 
 * Catatan      :: PGP RSA - Encrypt / Decrypt / Sign / Verify File
 *              :: Harap Didaftarkan Ke DI Container
 * 
 */

using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;

using Org.BouncyCastle.Bcpg;
using Org.BouncyCastle.Bcpg.OpenPgp;
using Org.BouncyCastle.Crypto;
using Org.BouncyCastle.Crypto.Generators;
using Org.BouncyCastle.Security;

namespace bifeldy_sd3_lib_452.Utilities {

    public interface IPgpRsa {
        bool IsValidPrivateKey(Stream privateKeyStream, char[] passphrase);
        bool IsValidPrivateKeyFile(string keyFilePath, char[] passphrase);
        bool IsValidPrivateKeyString(string keyFileString, char[] passphrase);
        bool IsValidPublicKey(Stream publicKeyStream);
        bool IsValidPublicKeyFile(string keyFilePath);
        bool IsValidPublicKeyString(string keyFileString);
        (string, string) GenerateKeyPairString(string identity, char[] passPhrase);
        void GenerateKeyPairFile(string identity, char[] passPhrase, string publicKeyPath, string privateKeyPath, Encoding encoding = null);
        Task<string> SignStreamToString(Stream stream, Stream privateKeyStream, char[] passPhrase = null);
        Task<string> SignStreamToStringWithPrivateKeyFile(Stream stream, string privateKeyPath, char[] passPhrase = null);
        Task<string> SignStreamToStringWithPrivateKeyString(Stream stream, string privateKeyString, char[] passPhrase = null);
        Task<string> SignFileToStringWithPrivateKeyFile(string filePath, string privateKeyPath, char[] passPhrase = null);
        Task<string> SignFileToStringWithPrivateKeyString(string filePath, string privateKeyString, char[] passPhrase = null);
        Task<bool> VerifyStreamWithSignatureString(Stream stream, Stream publicKeyStream, string base64Signature);
        Task<bool> VerifyStreamWithPublicKeyFileAndSignatureString(Stream stream, string publicKeyPath, string base64Signature);
        Task<bool> VerifyStreamWithPublicKeyStringAndSignatureString(Stream stream, string publicKeyString, string base64Signature);
        Task<bool> VerifyFileWithPublicKeyFileAndSignatureString(string filePath, string publicKeyPath, string base64Signature);
        Task<bool> VerifyFileWithPublicKeyStringAndSignatureString(string filePath, string publicKeyString, string base64Signature);
        Task SignFileDetached(FileStream fileStream, Stream privateKeyStream, string outputSigPath, char[] passPhrase = null);
        Task SignFileDetachedWithPrivateKeyFile(string filePath, string privateKeyPath, string outputSigPath, char[] passPhrase = null);
        Task SignFileDetachedWithPrivateKeyString(string filePath, string privateKeyString, string outputSigPath, char[] passPhrase = null);
        Task<bool> VerifyFileDetached(FileStream fileStream, Stream publicKeyStream, FileStream inputSigPath);
        Task<bool> VerifyFileDetachedWithPublicKeyFile(string filePath, string publicKeyPath, string inputSigPath);
        Task<bool> VerifyFileDetachedWithPublicKeyString(string filePath, string publicKeyString, string inputSigPath);
    }

    public sealed class CPgpRsa : IPgpRsa {

        private readonly ILogger _logger;

        public CPgpRsa(ILogger logger) {
            this._logger = logger;
        }

        private PgpSecretKey GetSigningSecretKey(PgpSecretKeyRingBundle bundle) {
            PgpSecretKey pgpSecKey = null;

            foreach (PgpSecretKeyRing kRing in bundle.GetKeyRings()) {
                foreach (PgpSecretKey key in kRing.GetSecretKeys()) {
                    if (key.IsSigningKey) {
                        pgpSecKey = key;
                    }
                }
            }

            if (pgpSecKey == null) {
                throw new Exception("No signing key found in the private keyring. Make sure the key allows signing and is exported with `--export-secret-keys`.");
            }

            return pgpSecKey;
        }

        private PgpSignatureList GetPgpSigList(PgpObjectFactory pgpFact) {
            PgpSignatureList sigList = null;

            PgpObject obj = pgpFact.NextPgpObject();
            if (obj is PgpCompressedData c1) {
                var pgpFact2 = new PgpObjectFactory(c1.GetDataStream());
                sigList = (PgpSignatureList)pgpFact2.NextPgpObject();
            }
            else {
                sigList = (PgpSignatureList)obj;
            }

            return sigList;
        }

        private PgpSignatureGenerator GetPgpSignatureGenerator(PgpSecretKey pgpSecKey, PgpPrivateKey pgpPrivKey) {
            var sGen = new PgpSignatureGenerator(pgpSecKey.PublicKey.Algorithm, HashAlgorithmTag.Sha256);
            sGen.InitSign(PgpSignature.BinaryDocument, pgpPrivKey);

            foreach (string userId in pgpSecKey.PublicKey.GetUserIds()) {
                var spGen = new PgpSignatureSubpacketGenerator();
                spGen.SetSignerUserId(false, userId);
                sGen.SetHashedSubpackets(spGen.Generate());
                break;
            }

            return sGen;
        }

        /* ** */

        public bool IsValidPrivateKey(Stream privateKeyStream, char[] passphrase) {
            try {
                using (Stream keyIn = PgpUtilities.GetDecoderStream(privateKeyStream)) {
                    var pgpSec = new PgpSecretKeyRingBundle(keyIn);
                    PgpSecretKey pgpSecKey = this.GetSigningSecretKey(pgpSec);
                    PgpPrivateKey privKey = pgpSecKey.ExtractPrivateKey(passphrase);
                    return privKey != null;
                }
            }
            catch {
                // Invalid key file, corrupted, wrong passphrase, or not a signing key
            }

            return false;
        }

        public bool IsValidPrivateKeyFile(string keyFilePath, char[] passphrase) {
            using (FileStream fileStream = File.OpenRead(keyFilePath)) {
                return this.IsValidPrivateKey(fileStream, passphrase);
            }
        }

        public bool IsValidPrivateKeyString(string keyFileString, char[] passphrase) {
            using (TextReader reader = new StringReader(keyFileString)) {
                using (var keyStream = new MemoryStream(Encoding.UTF8.GetBytes(keyFileString))) {
                    return this.IsValidPrivateKey(keyStream, passphrase);
                }
            }
        }

        public bool IsValidPublicKey(Stream publicKeyStream) {
            try {
                using (Stream keyIn = PgpUtilities.GetDecoderStream(publicKeyStream)) {
                    var pubKeyRing = new PgpPublicKeyRingBundle(keyIn);
                    foreach (PgpPublicKeyRing kRing in pubKeyRing.GetKeyRings()) {
                        foreach (PgpPublicKey key in kRing.GetPublicKeys()) {
                            if (key.IsEncryptionKey || key.IsMasterKey) {
                                return true;
                            }
                        }
                    }
                }
            }
            catch {
                // Invalid or corrupted public key
            }

            return false;
        }

        public bool IsValidPublicKeyFile(string keyFilePath) {
            using (FileStream fileStream = File.OpenRead(keyFilePath)) {
                return this.IsValidPublicKey(fileStream);
            }
        }

        public bool IsValidPublicKeyString(string keyFileString) {
            using (TextReader reader = new StringReader(keyFileString)) {
                using (var keyStream = new MemoryStream(Encoding.UTF8.GetBytes(keyFileString))) {
                    return this.IsValidPublicKey(keyStream);
                }
            }
        }

        /* ** */

        public (string, string) GenerateKeyPairString(string identity, char[] passPhrase) {
            IAsymmetricCipherKeyPairGenerator kpg = new RsaKeyPairGenerator();
            kpg.Init(new KeyGenerationParameters(new SecureRandom(), 2048));
            AsymmetricCipherKeyPair kp = kpg.GenerateKeyPair();

            var pgpKeyPair = new PgpKeyPair(PublicKeyAlgorithmTag.RsaGeneral, kp, DateTime.UtcNow);
            var subGen = new PgpSignatureSubpacketGenerator();
            var keyRingGen = new PgpKeyRingGenerator(
                PgpSignature.DefaultCertification,
                pgpKeyPair,
                identity,
                SymmetricKeyAlgorithmTag.Aes256,
                passPhrase,
                true,
                subGen.Generate(),
                null,
                new SecureRandom()
            );

            string publicKey, privateKey;

            using (var pubOut = new MemoryStream()) {
                using (var pubWriter = new ArmoredOutputStream(pubOut)) {
                    keyRingGen.GeneratePublicKeyRing().Encode(pubWriter);
                }

                publicKey = Encoding.UTF8.GetString(pubOut.ToArray());
            }

            using (var privOut = new MemoryStream()) {
                using (var privWriter = new ArmoredOutputStream(privOut)) {
                    keyRingGen.GenerateSecretKeyRing().Encode(privWriter);
                }

                privateKey = Encoding.UTF8.GetString(privOut.ToArray());
            }

            return (publicKey, privateKey);
        }

        public void GenerateKeyPairFile(string identity, char[] passPhrase, string publicKeyPath, string privateKeyPath, Encoding encoding = null) {
            (string publicKey, string privateKey) = this.GenerateKeyPairString(identity, passPhrase);

            File.WriteAllText(publicKeyPath, publicKey, encoding ?? Encoding.UTF8);
            File.WriteAllText(privateKeyPath, privateKey, encoding ?? Encoding.UTF8);
        }

        /* ** */

        public async Task<string> SignStreamToString(Stream stream, Stream privateKeyStream, char[] passPhrase = null) {
            using (Stream keyStream = PgpUtilities.GetDecoderStream(stream)) {
                var pgpSec = new PgpSecretKeyRingBundle(privateKeyStream);
                PgpSecretKey pgpSecKey = this.GetSigningSecretKey(pgpSec);

                char[] actualPassphrase = passPhrase ?? new char[0];
                PgpPrivateKey pgpPrivKey = pgpSecKey.ExtractPrivateKey(actualPassphrase);

                PgpSignatureGenerator sGen = this.GetPgpSignatureGenerator(pgpSecKey, pgpPrivKey);

                byte[] buf = new byte[8192];
                int len = 0;

                stream.Position = 0;
                while ((len = await stream.ReadAsync(buf, 0, buf.Length)) > 0) {
                    sGen.Update(buf, 0, len);
                }

                using (var memOut = new MemoryStream()) {
                    var bOut = new BcpgOutputStream(memOut);
                    sGen.Generate().Encode(bOut);
                    return Convert.ToBase64String(memOut.ToArray());
                }
            }
        }

        public async Task<string> SignStreamToStringWithPrivateKeyFile(Stream stream, string privateKeyPath, char[] passPhrase = null) {
            using (Stream keyStream = File.OpenRead(privateKeyPath)) {
                return await this.SignStreamToString(stream, keyStream, passPhrase);
            }
        }

        public async Task<string> SignStreamToStringWithPrivateKeyString(Stream stream, string privateKeyString, char[] passPhrase = null) {
            using (var keyStream = new MemoryStream(Encoding.UTF8.GetBytes(privateKeyString))) {
                return await this.SignStreamToString(stream, keyStream, passPhrase);
            }
        }

        public async Task<string> SignFileToStringWithPrivateKeyFile(string filePath, string privateKeyPath, char[] passPhrase = null) {
            using (FileStream fileStream = File.OpenRead(filePath)) {
                return await this.SignStreamToStringWithPrivateKeyFile(fileStream, privateKeyPath, passPhrase);
            }
        }

        public async Task<string> SignFileToStringWithPrivateKeyString(string filePath, string privateKeyString, char[] passPhrase = null) {
            using (FileStream fileStream = File.OpenRead(filePath)) {
                return await this.SignStreamToStringWithPrivateKeyString(fileStream, privateKeyString, passPhrase);
            }
        }

        /* ** */

        public async Task<bool> VerifyStreamWithSignatureString(Stream stream, Stream publicKeyStream, string base64Signature) {
            using (Stream keyStream = PgpUtilities.GetDecoderStream(stream)) {
                byte[] signatureBytes = Convert.FromBase64String(base64Signature);

                var pgpPubRing = new PgpPublicKeyRingBundle(publicKeyStream);
                var pgpFact = new PgpObjectFactory(signatureBytes);
                PgpSignatureList sigList = this.GetPgpSigList(pgpFact);

                PgpSignature sig = sigList[0];
                PgpPublicKey key = pgpPubRing.GetPublicKey(sig.KeyId);

                sig.InitVerify(key);

                byte[] buf = new byte[8192];
                int len = 0;

                stream.Position = 0;
                while ((len = await stream.ReadAsync(buf, 0, buf.Length)) > 0) {
                    sig.Update(buf, 0, len);
                }

                return sig.Verify();
            }
        }

        public async Task<bool> VerifyStreamWithPublicKeyFileAndSignatureString(Stream stream, string publicKeyPath, string base64Signature) {
            using (Stream keyStream = File.OpenRead(publicKeyPath)) {
                return await this.VerifyStreamWithSignatureString(stream, keyStream, base64Signature);
            }
        }

        public async Task<bool> VerifyStreamWithPublicKeyStringAndSignatureString(Stream stream, string publicKeyString, string base64Signature) {
            using (var keyStream = new MemoryStream(Encoding.UTF8.GetBytes(publicKeyString))) {
                return await this.VerifyStreamWithSignatureString(stream, keyStream, base64Signature);
            }
        }

        public async Task<bool> VerifyFileWithPublicKeyFileAndSignatureString(string filePath, string publicKeyPath, string base64Signature) {
            using (FileStream fileStream = File.OpenRead(filePath)) {
                return await this.VerifyStreamWithPublicKeyFileAndSignatureString(fileStream, publicKeyPath, base64Signature);
            }
        }

        public async Task<bool> VerifyFileWithPublicKeyStringAndSignatureString(string filePath, string publicKeyString, string base64Signature) {
            using (FileStream fileStream = File.OpenRead(filePath)) {
                return await this.VerifyStreamWithPublicKeyStringAndSignatureString(fileStream, publicKeyString, base64Signature);
            }
        }

        /* ** */

        public async Task SignFileDetached(FileStream fileStream, Stream privateKeyStream, string outputSigPath, char[] passPhrase = null) {
            using (Stream keyIn = PgpUtilities.GetDecoderStream(privateKeyStream)) {
                var pgpSec = new PgpSecretKeyRingBundle(keyIn);
                PgpSecretKey pgpSecKey = this.GetSigningSecretKey(pgpSec);

                char[] actualPassphrase = passPhrase ?? new char[0];
                PgpPrivateKey pgpPrivKey = pgpSecKey.ExtractPrivateKey(actualPassphrase);

                PgpSignatureGenerator sGen = this.GetPgpSignatureGenerator(pgpSecKey, pgpPrivKey);

                using (Stream outStream = File.Create(outputSigPath)) {
                    using (var aOut = new ArmoredOutputStream(outStream)) {
                        var bOut = new BcpgOutputStream(aOut);

                        string fileName = new FileInfo(fileStream.Name).Name;
                        ulong totalBytesRead = 0;
                        DateTime lastReportTime = DateTime.UtcNow;

                        byte[] buf = new byte[8192];
                        int len = 0;

                        fileStream.Position = 0;
                        while ((len = await fileStream.ReadAsync(buf, 0, buf.Length)) > 0) {
                            sGen.Update(buf, 0, len);
                            totalBytesRead += (ulong)len;

                            if ((DateTime.UtcNow - lastReportTime).TotalSeconds >= 1) {
                                double percentage = (double)totalBytesRead / fileStream.Length * 100;
                                this._logger.WriteInfo($"{this.GetType().Name}SignFileDetached", $"Reading File {fileName} :: {percentage:F2}%");
                                lastReportTime = DateTime.UtcNow;
                            }
                        }

                        this._logger.WriteInfo($"{this.GetType().Name}SignFileDetached", $"Reading File {fileName} :: 100%");
                        sGen.Generate().Encode(bOut);
                    }
                }
            }
        }

        public async Task SignFileDetachedWithPrivateKeyFile(string filePath, string privateKeyPath, string outputSigPath, char[] passPhrase = null) {
            using (FileStream fileStream = File.OpenRead(filePath)) {
                using (Stream privateKeyStream = File.OpenRead(privateKeyPath)) {
                    await this.SignFileDetached(fileStream, privateKeyStream, outputSigPath, passPhrase);
                }
            }
        }

        public async Task SignFileDetachedWithPrivateKeyString(string filePath, string privateKeyString, string outputSigPath, char[] passPhrase = null) {
            using (FileStream fileStream = File.OpenRead(filePath)) {
                using (var keyStream = new MemoryStream(Encoding.UTF8.GetBytes(privateKeyString))) {
                    await this.SignFileDetached(fileStream, keyStream, outputSigPath, passPhrase);
                }
            }
        }

        /* ** */

        public async Task<bool> VerifyFileDetached(FileStream fileStream, Stream publicKeyStream, FileStream inputSigPath) {
            using (Stream keyStream = PgpUtilities.GetDecoderStream(publicKeyStream)) {
                using (Stream sigStream = PgpUtilities.GetDecoderStream(inputSigPath)) {
                    var pgpPubRing = new PgpPublicKeyRingBundle(keyStream);
                    var pgpFact = new PgpObjectFactory(sigStream);
                    PgpSignatureList sigList = this.GetPgpSigList(pgpFact);

                    int i = 0;
                    do {
                        PgpSignature sig = sigList[i];
                        PgpPublicKey key = pgpPubRing.GetPublicKey(sig.KeyId);

                        if (key != null) {
                            sig.InitVerify(key);

                            string fileName = new FileInfo(fileStream.Name).Name;
                            ulong totalBytesRead = 0;
                            DateTime lastReportTime = DateTime.UtcNow;

                            byte[] buf = new byte[8192];
                            int len = 0;

                            fileStream.Position = 0;
                            while ((len = await fileStream.ReadAsync(buf, 0, buf.Length)) > 0) {
                                sig.Update(buf, 0, len);
                                totalBytesRead += (ulong)len;

                                if ((DateTime.UtcNow - lastReportTime).TotalSeconds >= 1) {
                                    double percentage = (double)totalBytesRead / fileStream.Length * 100;
                                    this._logger.WriteInfo($"{this.GetType().Name}VerifyFileDetached", $"Reading File {fileName} :: {percentage:F2}%");
                                    lastReportTime = DateTime.UtcNow;
                                }
                            }

                            this._logger.WriteInfo($"{this.GetType().Name}VerifyFileDetached", $"Reading File {fileName} :: 100%");

                            if (sig.Verify()) {
                                return true;
                            }
                        }

                        i++;
                    }
                    while (i < sigList.Count);
                    return false;
                }
            }
        }

        public async Task<bool> VerifyFileDetachedWithPublicKeyFile(string filePath, string publicKeyPath, string inputSigPath) {
            using (FileStream fileStream = File.OpenRead(filePath)) {
                using (FileStream keyStream = File.OpenRead(publicKeyPath)) {
                    using (FileStream sigStream = File.OpenRead(inputSigPath)) {
                        return await this.VerifyFileDetached(fileStream, keyStream, sigStream);
                    }
                }
            }
        }

        public async Task<bool> VerifyFileDetachedWithPublicKeyString(string filePath, string publicKeyString, string inputSigPath) {
            using (FileStream fileStream = File.OpenRead(filePath)) {
                using (var keyStream = new MemoryStream(Encoding.UTF8.GetBytes(publicKeyString))) {
                    using (FileStream sigStream = File.OpenRead(inputSigPath)) {
                        return await this.VerifyFileDetached(fileStream, keyStream, sigStream);
                    }
                }
            }
        }

    }

}
