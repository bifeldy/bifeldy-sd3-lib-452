/**
 * 
 * Author       :: Basilius Bias Astho Christyono
 * Phone        :: (+62) 889 236 6466
 * 
 * Department   :: IT SD 03
 * Mail         :: bias@indomaret.co.id
 * 
 * Catatan      :: Amazon S3
 *              :: Harap Didaftarkan Ke DI Container
 * 
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

using Amazon;
using Amazon.S3;
using Amazon.S3.Model;

using bifeldy_sd3_lib_452.Models;

namespace bifeldy_sd3_lib_452.Utilities {

    public interface IAmazonS3 {
        string GetAccessKey();
        string GetSecretKey();
        void LoadCredential(string pathFile, bool isEncrypted = false);
        void InitializeClient(string region = null);
        Task<List<AwsS3Bucket>> ListBucketsAsync(string region = null);
        Task<(List<AwsS3Prefix>, List<AwsS3Object>)> ListObjectsAsync(string bucketName, string prefix = "", string region = null);
        Task<AwsS3GetObjectResponse> GetFileResponse(string bucketName, string fileKey, long startByte = 0, string region = null);
        string GeneratePresignedUrl(string bucketName, string fileKey, double expirationHours = 1, string region = null);
    }

    public sealed class CAmazonS3 : IAmazonS3 {

        private readonly ILogger _logger;
        private readonly IChiper _chiper;
        private readonly IConverter _converter;

        private string credentialPath = string.Empty;

        private AmazonS3Client s3Client = null;

        private string accessKey;
        private string secretKey;

        public CAmazonS3(ILogger logger, IChiper chiper, IConverter converter) {
            this._logger = logger;
            this._chiper = chiper;
            this._converter = converter;
        }

        public string GetAccessKey() => this.accessKey;
        public string GetSecretKey() => this.secretKey;

        public void LoadCredential(string pathFile, bool isEncrypted = false) {
            this.credentialPath = pathFile;
            if (string.IsNullOrEmpty(this.credentialPath) || !File.Exists(this.credentialPath)) {
                throw new Exception("Lokasi file credential tidak valid");
            }

            string text = File.ReadAllText(this.credentialPath);
            this._logger.WriteInfo($"{this.GetType().Name}Credential", text);

            if (isEncrypted) {
                text = this._chiper.DecryptText(text);
            }

            IDictionary<string, string> json = this._converter.JsonToObject<Dictionary<string, string>>(text);
            _ = json.TryGetValue("access_key", out this.accessKey);
            _ = json.TryGetValue("secret_key", out this.secretKey);

            if (!isEncrypted) {
                File.WriteAllText($"{this.credentialPath}.txt", this._chiper.EncryptText(text));
            }
        }

        public void InitializeClient(string region = null) {
            if (string.IsNullOrEmpty(this.accessKey) || string.IsNullOrEmpty(this.secretKey)) {
                this.LoadCredential(this.credentialPath, this.credentialPath.ToLower().EndsWith(".txt"));
            }

            var s3Config = new AmazonS3Config() {
                RegionEndpoint = string.IsNullOrEmpty(region) ? RegionEndpoint.APSoutheast3 : RegionEndpoint.GetBySystemName(region)
            };

            this.s3Client = new AmazonS3Client(this.accessKey, this.secretKey, s3Config);

            this._logger.WriteInfo($"{this.GetType().Name}Client", s3Config.RegionEndpoint.SystemName);
        }

        public async Task<List<AwsS3Bucket>> ListBucketsAsync(string region = null) {
            if (this.s3Client == null) {
                this.InitializeClient(region);
            }

            this._logger.WriteInfo($"{this.GetType().Name}ListBuckets", "Mengambil daftar bucket");

            ListBucketsResponse response = await this.s3Client.ListBucketsAsync();

            var result = new List<AwsS3Bucket>();
            foreach (S3Bucket b in response.Buckets) {
                result.Add(new AwsS3Bucket() {
                    BucketArn = b.BucketArn,
                    CreationDate = b.CreationDate,
                    BucketName = b.BucketName,
                    BucketRegion = b.BucketRegion
                });
            }

            return result;
        }

        public async Task<(List<AwsS3Prefix>, List<AwsS3Object>)> ListObjectsAsync(string bucketName, string prefix = "", string region = null) {
            if (this.s3Client == null) {
                this.InitializeClient(region);
            }

            this._logger.WriteInfo($"{this.GetType().Name}ListObjects", $"Mengambil object di {bucketName}/{prefix}");

            var request = new ListObjectsV2Request() {
                BucketName = bucketName,
                Prefix = prefix,
                Delimiter = "/"
            };

            var resultPrefixes = new List<AwsS3Prefix>();
            var resultObjects = new List<AwsS3Object>();
            ListObjectsV2Response response;
            ulong pageNum = 1;

            do {
                this._logger.WriteInfo($"{this.GetType().Name}LoadObjectPage", $"{pageNum}");
                response = await this.s3Client.ListObjectsV2Async(request);

                foreach (string p in response.CommonPrefixes) {
                    resultPrefixes.Add(new AwsS3Prefix() {
                        BucketName = bucketName,
                        Prefix = p
                    });
                }

                foreach (S3Object o in response.S3Objects) {
                    if (o.Key == prefix) {
                        continue;
                    }

                    resultObjects.Add(new AwsS3Object() {
                        ChecksumAlgorithm = o.ChecksumAlgorithm,
                        ETag = o.ETag,
                        BucketName = o.BucketName,
                        Key = o.Key,
                        LastModified = o.LastModified,
                        OwnerId = o.Owner?.Id,
                        OwnerDisplayName = o.Owner?.DisplayName,
                        IsRestoreInProgress = o.RestoreStatus?.IsRestoreInProgress ?? false,
                        Size = o.Size,
                        StorageClass = o.StorageClass?.Value,
                        ChecksumType = o.ChecksumType?.Value
                    });
                }

                request.ContinuationToken = response.NextContinuationToken;
                pageNum++;

            }
            while (response.IsTruncated);

            return (resultPrefixes, resultObjects);
        }

        public async Task<AwsS3GetObjectResponse> GetFileResponse(string bucketName, string fileKey, long startByte = 0, string region = null) {
            if (this.s3Client == null) {
                this.InitializeClient(region);
            }

            var request = new GetObjectRequest() {
                BucketName = bucketName,
                Key = fileKey
            };

            if (startByte > 0) {
                request.ByteRange = new ByteRange(startByte, long.MaxValue);
            }

            this._logger.WriteInfo($"{this.GetType().Name}GetStart", $"{bucketName}/{fileKey}");

            GetObjectResponse res = await this.s3Client.GetObjectAsync(request);

            var myRes = new AwsS3GetObjectResponse(res) {
                ResponseStream = res.ResponseStream,
                ContentLength = res.ContentLength,
                HttpStatusCode = res.HttpStatusCode,
                AcceptRanges = res.AcceptRanges,
                BucketKeyEnabled = res.BucketKeyEnabled,
                BucketName = res.BucketName,
                ChecksumCRC32 = res.ChecksumCRC32,
                ChecksumCRC32C = res.ChecksumCRC32C,
                ChecksumCRC64NVME = res.ChecksumCRC64NVME,
                ChecksumMD5 = res.ChecksumMD5,
                ChecksumSHA1 = res.ChecksumSHA1,
                ChecksumSHA256 = res.ChecksumSHA256,
                ChecksumSHA512 = res.ChecksumSHA512,
                ChecksumType = res.ChecksumType?.Value,
                ChecksumXXHASH128 = res.ChecksumXXHASH128,
                ChecksumXXHASH3 = res.ChecksumXXHASH3,
                ChecksumXXHASH64 = res.ChecksumXXHASH64,
                ContentRange = res.ContentRange,
                DeleteMarker = res.DeleteMarker,
                ETag = res.ETag,
                ExpirationRuleId = res.Expiration?.RuleId,
                ExpirationDateUtc = res.Expiration?.ExpiryDateUtc,
                ExpiresString = res.ExpiresString,
                Key = res.Key,
                LastModified = res.LastModified,
                MissingMeta = res.MissingMeta,
                ObjectLockLegalHoldStatus = res.ObjectLockLegalHoldStatus?.Value,
                ObjectLockMode = res.ObjectLockMode?.Value,
                ObjectLockRetainUntilDate = res.ObjectLockRetainUntilDate,
                PartsCount = res.PartsCount,
                ReplicationStatus = res.ReplicationStatus?.Value,
                RequestCharged = res.RequestCharged?.Value,
                RestoreExpiration = res.RestoreExpiration,
                RestoreInProgress = res.RestoreInProgress,
                ServerSideEncryptionCustomerMethod = res.ServerSideEncryptionCustomerMethod?.Value,
                ServerSideEncryptionKeyManagementServiceKeyId = res.ServerSideEncryptionKeyManagementServiceKeyId,
                ServerSideEncryptionMethod = res.ServerSideEncryptionMethod?.Value,
                StorageClass = res.StorageClass?.Value,
                TagCount = res.TagCount,
                VersionId = res.VersionId,
                WebsiteRedirectLocation = res.WebsiteRedirectLocation
            };

            foreach (string key in res.Headers.Keys) {
                myRes.Headers[key] = res.Headers[key];
            }

            foreach (string key in res.Metadata.Keys) {
                myRes.Metadata[key] = res.Metadata[key];
            }

            return myRes;
        }

        public string GeneratePresignedUrl(string bucketName, string fileKey, double expirationHours = 1, string region = null) {
            if (this.s3Client == null) {
                this.InitializeClient(region);
            }

            var request = new GetPreSignedUrlRequest() {
                BucketName = bucketName,
                Key = fileKey,
                Expires = DateTime.UtcNow.AddHours(expirationHours)
            };

            return this.s3Client.GetPreSignedURL(request);
        }

    }

}