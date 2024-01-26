/**
 * 
 * Author       :: Basilius Bias Astho Christyono
 * Phone        :: (+62) 889 236 6466
 * 
 * Department   :: IT SD 03
 * Mail         :: bias@indomaret.co.id
 * 
 * Catatan      :: Google Cloud Storage
 *              :: Harap Didaftarkan Ke DI Container
 * 
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using System.Web;

using Google.Apis.Auth.OAuth2;
using Google.Apis.Download;
using Google.Apis.Services;
using Google.Apis.Storage.v1;
using Google.Apis.Upload;

using Google.Cloud.Storage.V1;

using bifeldy_sd3_lib_452.Models;
using static Google.Apis.Storage.v1.ObjectsResource;

namespace bifeldy_sd3_lib_452.Utilities {

    public interface IGoogleCloudStorage {
        void LoadCredential(string pathFile = null, bool isEncrypted = false);
        void InitializeClient();
        Task<List<GcsBucket>> ListAllBuckets();
        Task<List<GcsObject>> ListAllObjects(string path, string prefix = "", string delimiter = "");
        GcsMediaUpload GenerateUploadMedia(FileInfo fileInfo, string bucketName, Stream stream);
        Task<Uri> CreateUploadUri(GcsMediaUpload mediaUpload);
        Task<CGcsUploadProgress> UploadFile(GcsMediaUpload mediaUpload, Uri uploadSession = null, Action<CGcsUploadProgress> uploadProgress = null);
        Task DownloadFile(GcsObject fileObj, string fileLocalPath, Action<CGcsDownloadProgress> downloadProgress = null);
        Task<string> CreateDownloadUrlSigned(GcsObject fileObj, TimeSpan expiredDurationFromNow);
        Task<string> CreateDownloadUrlSigned(GcsObject fileObj, DateTime expiryDateTime);
    }

    public sealed class CGoogleCloudStorage : IGoogleCloudStorage {

        private readonly ILogger _logger;
        private readonly IChiper _chiper;
        private readonly IConverter _converter;

        private string credentialPath = string.Empty;
        private string projectId = string.Empty;

        private GoogleCredential googleCredential = null;
        private StorageService storageService = null;
        private UrlSigner urlSigner = null;

        public CGoogleCloudStorage(ILogger logger, IChiper chiper, IConverter converter) {
            _logger = logger;
            _chiper = chiper;
            _converter = converter;
        }

        public void LoadCredential(string pathFile, bool isEncrypted = false) {
            credentialPath = pathFile;
            if (string.IsNullOrEmpty(credentialPath) || !File.Exists(credentialPath)) {
                throw new Exception("Lokasi file credential.json tidak valid");
            }
            string text = File.ReadAllText(credentialPath);
            _logger.WriteInfo($"{GetType().Name}Credential", text);
            if (isEncrypted) {
                text = _chiper.DecryptText(text);
            }
            IDictionary<string, string> json = _converter.JsonToObject<Dictionary<string, string>>(text);
            json.TryGetValue("project_id", out projectId);
            googleCredential = GoogleCredential.FromJson(text).CreateScoped(StorageService.Scope.DevstorageFullControl);
            using (MemoryStream ms = new MemoryStream()) {
                using (StreamWriter writer = new StreamWriter(ms)) {
                    writer.Write(text);
                    writer.Flush();
                    ms.Position = 0;
                    urlSigner = UrlSigner.FromServiceAccountData(ms);
                }
            }
        }

        public void InitializeClient() {
            if (googleCredential == null) {
                LoadCredential(credentialPath);
            }
            storageService = new StorageService(
                new BaseClientService.Initializer() {
                    HttpClientInitializer = googleCredential,
                    ApplicationName = projectId
                }
            );
            _logger.WriteInfo($"{GetType().Name}Client", storageService.Name);
        }

        public async Task<List<GcsBucket>> ListAllBuckets() {
            if (storageService == null) {
                InitializeClient();
            }
            List<GcsBucket> allBuckets = new List<GcsBucket>();

            BucketsResource.ListRequest request = storageService.Buckets.List(storageService.ApplicationName);
            request.Fields = "nextPageToken, items";

            ulong pageNum = 1;
            string pageToken = null;
            do {
                _logger.WriteInfo($"{GetType().Name}LoadBucketPage", $"{pageNum}");

                request.PageToken = pageToken;
                Google.Apis.Storage.v1.Data.Buckets buckets = await request.ExecuteAsync();
                pageToken = buckets.NextPageToken;
                if (buckets != null) {
                    if (buckets.Items != null) {
                        foreach (Google.Apis.Storage.v1.Data.Bucket bucket in buckets.Items) {
                            allBuckets.Add(new GcsBucket {
                                Website = bucket.Website,
                                Versioning = bucket.Versioning,
                                Updated = bucket.Updated,
                                UpdatedRaw = bucket.UpdatedRaw,
                                TimeCreated = bucket.TimeCreated,
                                TimeCreatedRaw = bucket.TimeCreatedRaw,
                                StorageClass = bucket.StorageClass,
                                SelfLink = bucket.SelfLink,
                                RetentionPolicy = bucket.RetentionPolicy,
                                Owner = bucket.Owner,
                                Name = bucket.Name,
                                Metageneration = bucket.Metageneration,
                                Logging = bucket.Logging,
                                ProjectNumber = bucket.ProjectNumber,
                                Location = bucket.Location,
                                Acl = bucket.Acl,
                                Billing = bucket.Billing,
                                LocationType = bucket.LocationType,
                                DefaultEventBasedHold = bucket.DefaultEventBasedHold,
                                DefaultObjectAcl = bucket.DefaultObjectAcl,
                                Encryption = bucket.Encryption,
                                Cors = bucket.Cors,
                                IamConfiguration = bucket.IamConfiguration,
                                Id = bucket.Id,
                                Kind = bucket.Kind,
                                Labels = bucket.Labels,
                                Lifecycle = bucket.Lifecycle,
                                ETag = bucket.ETag
                            });
                        }
                    }
                }

                pageNum++;
            }
            while (pageToken != null);

            return allBuckets;
        }

        public async Task<List<GcsObject>> ListAllObjects(string path, string prefix = "", string delimiter = "") {
            if (storageService == null) {
                InitializeClient();
            }
            List<GcsObject> allObjects = new List<GcsObject>();

            ObjectsResource.ListRequest request = storageService.Objects.List(path);
            request.Fields = "nextPageToken, items";
            if (!string.IsNullOrEmpty(prefix)) {
                request.Prefix = prefix;
            }
            if (!string.IsNullOrEmpty(delimiter)) {
                request.Delimiter = delimiter;
            }

            ulong pageNum = 1;
            string pageToken = null;
            do {
                _logger.WriteInfo($"{GetType().Name}LoadObjectPage", $"{pageNum}");

                request.PageToken = pageToken;
                Google.Apis.Storage.v1.Data.Objects objects = await request.ExecuteAsync();
                pageToken = objects.NextPageToken;
                if (objects != null) {
                    if (objects.Items != null) {
                        foreach (Google.Apis.Storage.v1.Data.Object obj in objects.Items) {
                            allObjects.Add(new GcsObject {
                                Owner = obj.Owner,
                                RetentionExpirationTimeRaw = obj.RetentionExpirationTimeRaw,
                                RetentionExpirationTime = obj.RetentionExpirationTime,
                                SelfLink = obj.SelfLink,
                                Size = obj.Size,
                                StorageClass = obj.StorageClass,
                                TemporaryHold = obj.TemporaryHold,
                                TimeCreatedRaw = obj.TimeCreatedRaw,
                                TimeCreated = obj.TimeCreated,
                                TimeDeletedRaw = obj.TimeDeletedRaw,
                                TimeDeleted = obj.TimeDeleted,
                                TimeStorageClassUpdatedRaw = obj.TimeStorageClassUpdatedRaw,
                                TimeStorageClassUpdated = obj.TimeStorageClassUpdated,
                                UpdatedRaw = obj.UpdatedRaw,
                                Updated = obj.Updated,
                                Name = obj.Name,
                                Metageneration = obj.Metageneration,
                                Metadata = obj.Metadata,
                                Crc32c = obj.Crc32c,
                                CacheControl = obj.CacheControl,
                                ComponentCount = obj.ComponentCount,
                                ContentDisposition = obj.ContentDisposition,
                                ContentEncoding = obj.ContentEncoding,
                                ContentLanguage = obj.ContentLanguage,
                                ContentType = obj.ContentType,
                                MediaLink = obj.MediaLink,
                                CustomerEncryption = obj.CustomerEncryption,
                                ETag = obj.ETag,
                                EventBasedHold = obj.EventBasedHold,
                                Generation = obj.Generation,
                                Id = obj.Id,
                                Kind = obj.Kind,
                                KmsKeyName = obj.KmsKeyName,
                                Md5Hash = obj.Md5Hash,
                                Bucket = obj.Bucket,
                                Acl = obj.Acl,
                            });
                        }
                    }
                }

                pageNum++;
            }
            while (pageToken != null);

            return allObjects;
        }

        public GcsMediaUpload GenerateUploadMedia(FileInfo fileInfo, string bucketName, Stream stream) {
            GcsObject obj = new GcsObject {
                Name = fileInfo.Name,
                Bucket = bucketName,
                ContentType = MimeMapping.GetMimeMapping(fileInfo.Name),
            };

            GcsMediaUpload mu = new GcsMediaUpload(storageService, obj, bucketName, stream, obj.ContentType);
            mu.Fields = "id, name, size, contentType";
            mu.ChunkSize = ResumableUpload.MinimumChunkSize;
            return mu;
        }

        public async Task<Uri> CreateUploadUri(GcsMediaUpload mediaUpload) {
            return await mediaUpload.InitiateSessionAsync();
        }

        public async Task<CGcsUploadProgress> UploadFile(GcsMediaUpload mediaUpload, Uri uploadSession = null, Action<CGcsUploadProgress> uploadProgress = null) {
            if (uploadSession == null) {
                uploadSession = await CreateUploadUri(mediaUpload);
            }
            if (uploadProgress != null) {
                mediaUpload.ProgressChanged += (progressNew) => {
                    Enum.TryParse(progressNew.Status.ToString(), out EGcsUploadStatus progressStatus);
                    CGcsUploadProgress upPrgs = new CGcsUploadProgress {
                        Status = progressStatus,
                        BytesSent = progressNew.BytesSent,
                        Exception = progressNew.Exception
                    };
                    uploadProgress(upPrgs);
                };
            }

            _logger.WriteInfo($"{GetType().Name}UploadStart", $"{mediaUpload.Body.Name} ===>>> {mediaUpload.Bucket} :: {mediaUpload.Body.Size} Bytes");
            IUploadProgress result = await mediaUpload.ResumeAsync(uploadSession);
            _logger.WriteInfo($"{GetType().Name}UploadCompleted", $"{mediaUpload.Body.Name} ===>>> {mediaUpload.Bucket} :: 100 %");

            Enum.TryParse(result.Status.ToString(), out EGcsUploadStatus uploadStatus);
            return  new CGcsUploadProgress {
                Status = uploadStatus,
                BytesSent = result.BytesSent,
                Exception = result.Exception
            };
        }

        public async Task DownloadFile(GcsObject fileObj, string fileLocalPath, Action<CGcsDownloadProgress> downloadProgress = null) {
            ObjectsResource.GetRequest request = storageService.Objects.Get(fileObj.Bucket, fileObj.Name);

            request.MediaDownloader.ChunkSize = ResumableUpload.MinimumChunkSize;

            if (downloadProgress != null) {
                request.MediaDownloader.ProgressChanged += (progressNew) => {
                    Enum.TryParse(progressNew.Status.ToString(), out EGcsDownloadStatus progressStatus);
                    CGcsDownloadProgress dwPrgs = new CGcsDownloadProgress {
                        Status = progressStatus,
                        BytesDownloaded = progressNew.BytesDownloaded,
                        Exception = progressNew.Exception
                    };
                    downloadProgress(dwPrgs);
                };
            }

            _logger.WriteInfo($"{GetType().Name}DownloadStart", $"{fileObj.Bucket}/{fileObj.Name} <<<=== {fileLocalPath} :: {fileObj.Size} Bytes");

            using (FileStream fs = new FileStream(fileLocalPath, FileMode.Create, FileAccess.Write)) {
                await request.DownloadAsync(fs);
                _logger.WriteInfo($"{GetType().Name}DownloadCompleted", $"{fileObj.Bucket}/{fileObj.Name} <<<=== {fileLocalPath} :: 100 %");
            }
        }

        public async Task<string> CreateDownloadUrlSigned(GcsObject fileObj, TimeSpan expiredDurationFromNow) {
            string ddl = await urlSigner.SignAsync(fileObj.Bucket, fileObj.Name, expiredDurationFromNow);
            _logger.WriteInfo($"{GetType().Name}DirectDownloadLinkTimeSpan", ddl);
            return ddl;
        }

        public async Task<string> CreateDownloadUrlSigned(GcsObject fileObj, DateTime expiryDateTime) {
            string ddl = await urlSigner.SignAsync(fileObj.Bucket, fileObj.Name, expiryDateTime);
            _logger.WriteInfo($"{GetType().Name}DirectDownloadLinkDateTime", ddl);
            return ddl;
        }

    }

}
