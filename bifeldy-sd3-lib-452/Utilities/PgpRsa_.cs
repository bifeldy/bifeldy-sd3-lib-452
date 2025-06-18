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

using Org.BouncyCastle.Bcpg;
using Org.BouncyCastle.Bcpg.OpenPgp;
using Org.BouncyCastle.Crypto;
using Org.BouncyCastle.Crypto.Generators;
using Org.BouncyCastle.Security;

namespace bifeldy_sd3_lib_452.Utilities {

    public interface IPgpRsa {
        bool IsValidPrivateKey(string keyFilePath, char[] passphrase);
        bool IsValidPublicKey(string keyFilePath);
        bool GenerateKeyPair(string identity, char[] passPhrase, string publicKeyPath, string privateKeyPath);
        string SignStreamToString(Stream inputStream, Stream privateKeyStream, char[] passPhrase = null);
        bool VerifyStreamWithSignatureString(Stream inputStream, Stream publicKeyStream, string base64Signature);
        string SignFileToString(string filePath, string privateKeyPath, char[] passPhrase = null);
        bool VerifyFileWithSignatureString(string filePath, string publicKeyPath, string base64Signature);
        void SignFileDetached(string filePath, string privateKeyPath, string outputSigPath, char[] passPhrase = null);
        bool VerifyFileDetached(string filePath, string publicKeyPath, string signaturePath);
    }

    public sealed class CPgpRsa : IPgpRsa {

        public CPgpRsa() {
            //
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
                throw new InvalidOperationException("No signing key found in the private keyring. Make sure the key allows signing and is exported with `--export-secret-keys`.");
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

        public bool IsValidPrivateKey(string keyFilePath, char[] passphrase) {
            try {
                using (Stream keyIn = PgpUtilities.GetDecoderStream(File.OpenRead(keyFilePath))) {
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

        public bool IsValidPublicKey(string keyFilePath) {
            try {
                using (Stream keyIn = PgpUtilities.GetDecoderStream(File.OpenRead(keyFilePath))) {
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

        public bool GenerateKeyPair(string identity, char[] passPhrase, string publicKeyPath, string privateKeyPath) {
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

            using (FileStream pubOut = File.Create(publicKeyPath)) {
                var armoredPub = new ArmoredOutputStream(pubOut);
                keyRingGen.GeneratePublicKeyRing().Encode(armoredPub);
                armoredPub.Close();
            }

            using (FileStream privOut = File.Create(privateKeyPath)) {
                var armoredPriv = new ArmoredOutputStream(privOut);
                keyRingGen.GenerateSecretKeyRing().Encode(armoredPriv);
                armoredPriv.Close();
            }

            bool privateKeyValid = this.IsValidPrivateKey(privateKeyPath, passPhrase);
            bool publicKeyValid = this.IsValidPublicKey(publicKeyPath);
            return privateKeyValid && publicKeyValid;
        }

        public string SignStreamToString(Stream inputStream, Stream privateKeyStream, char[] passPhrase = null) {
            var pgpSec = new PgpSecretKeyRingBundle(privateKeyStream);
            PgpSecretKey pgpSecKey = this.GetSigningSecretKey(pgpSec);

            char[] actualPassphrase = passPhrase ?? new char[0];
            PgpPrivateKey pgpPrivKey = pgpSecKey.ExtractPrivateKey(actualPassphrase);

            PgpSignatureGenerator sGen = this.GetPgpSignatureGenerator(pgpSecKey, pgpPrivKey);

            byte[] buf = new byte[8192];
            int len = 0;
            while ((len = inputStream.Read(buf, 0, buf.Length)) > 0) {
                sGen.Update(buf, 0, len);
            }

            using (var memOut = new MemoryStream()) {
                var bOut = new BcpgOutputStream(memOut);
                sGen.Generate().Encode(bOut);
                return Convert.ToBase64String(memOut.ToArray());
            }
        }

        public bool VerifyStreamWithSignatureString(Stream inputStream, Stream publicKeyStream, string base64Signature) {
            byte[] signatureBytes = Convert.FromBase64String(base64Signature);

            var pgpPubRing = new PgpPublicKeyRingBundle(publicKeyStream);
            var pgpFact = new PgpObjectFactory(signatureBytes);
            PgpSignatureList sigList = this.GetPgpSigList(pgpFact);

            PgpSignature sig = sigList[0];
            PgpPublicKey key = pgpPubRing.GetPublicKey(sig.KeyId);

            sig.InitVerify(key);

            byte[] buf = new byte[8192];
            int len = 0;
            while ((len = inputStream.Read(buf, 0, buf.Length)) > 0) {
                sig.Update(buf, 0, len);
            }

            return sig.Verify();
        }

        public string SignFileToString(string filePath, string privateKeyPath, char[] passPhrase = null) {
            using (Stream inputStream = File.OpenRead(filePath)) {
                using (Stream keyStream = PgpUtilities.GetDecoderStream(File.OpenRead(privateKeyPath))) {
                    return this.SignStreamToString(inputStream, keyStream, passPhrase);
                }
            }
        }

        public bool VerifyFileWithSignatureString(string filePath, string publicKeyPath, string base64Signature) {
            using (Stream inputFile = File.OpenRead(filePath)) {
                using (Stream keyStream = PgpUtilities.GetDecoderStream(File.OpenRead(publicKeyPath))) {
                    return this.VerifyStreamWithSignatureString(inputFile, keyStream, base64Signature);
                }
            }
        }

        public void SignFileDetached(string filePath, string privateKeyPath, string outputSigPath, char[] passPhrase = null) {
            using (Stream keyIn = PgpUtilities.GetDecoderStream(File.OpenRead(privateKeyPath))) {
                var pgpSec = new PgpSecretKeyRingBundle(keyIn);
                PgpSecretKey pgpSecKey = this.GetSigningSecretKey(pgpSec);

                char[] actualPassphrase = passPhrase ?? new char[0];
                PgpPrivateKey pgpPrivKey = pgpSecKey.ExtractPrivateKey(actualPassphrase);

                PgpSignatureGenerator sGen = this.GetPgpSignatureGenerator(pgpSecKey, pgpPrivKey);

                using (Stream fIn = File.OpenRead(filePath)) {
                    using (Stream outStream = File.Create(outputSigPath)) {
                        using (var aOut = new ArmoredOutputStream(outStream)) {
                            var bOut = new BcpgOutputStream(aOut);

                            byte[] buf = new byte[8192];
                            int len = 0;
                            while ((len = fIn.Read(buf, 0, buf.Length)) > 0) {
                                sGen.Update(buf, 0, len);
                            }

                            sGen.Generate().Encode(bOut);
                        }
                    }
                }
            }
        }

        public bool VerifyFileDetached(string filePath, string publicKeyPath, string signaturePath) {
            using (FileStream keyIn = File.OpenRead(publicKeyPath)) {
                using (Stream inputSig = PgpUtilities.GetDecoderStream(File.OpenRead(signaturePath))) {
                    using (Stream inputFile = File.OpenRead(filePath)) {
                        var pgpPubRing = new PgpPublicKeyRingBundle(PgpUtilities.GetDecoderStream(keyIn));
                        var pgpFact = new PgpObjectFactory(inputSig);
                        PgpSignatureList sigList = this.GetPgpSigList(pgpFact);

                        PgpSignature sig = sigList[0];
                        PgpPublicKey key = pgpPubRing.GetPublicKey(sig.KeyId);

                        sig.InitVerify(key);

                        byte[] buf = new byte[8192];
                        int len = 0;
                        while ((len = inputFile.Read(buf, 0, buf.Length)) > 0) {
                            sig.Update(buf, 0, len);
                        }

                        return sig.Verify();
                    }
                }
            }
        }

    }

}
