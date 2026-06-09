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
using System.Linq;
using System.Net.Http.Headers;
using System.Threading.Tasks;

using bifeldy_sd3_lib_452.Models;

using Google.Apis.Auth.OAuth2;
using Google.Apis.Download;
using Google.Apis.Upload;
using Google.Apis.Services;
using Google.Apis.Storage.v1;
using Google.Apis.Storagetransfer.v1;
using Google.Apis.Storagetransfer.v1.Data;
using Google.Cloud.Storage.V1;
using static Google.Apis.Storagetransfer.v1.TransferJobsResource;

namespace bifeldy_sd3_lib_452.Utilities {

    public interface IGoogleCloudStorage {
        void LoadCredential(string pathFile = null, bool isEncrypted = false);
        void InitializeClient();
        Task<List<GcsBucket>> ListAllBuckets();
        Task<(List<GcsPrefix>, List<GcsObject>)> ListAllObjects(string bucketName, string prefix = "", string delimiter = "");
        GcsMediaUpload GenerateUploadMedia(FileInfo fileInfo, string bucketName, string folderName = null);
        GcsMediaUpload GenerateUploadMedia(string fileKey, string bucketName, Stream stream, string contentType, string folderName = null);
        Task<Uri> CreateUploadUri(GcsMediaUpload mediaUpload);
        Task<CGcsUploadProgress> UploadFile(GcsMediaUpload mediaUpload, Uri uploadSession = null, Action<CGcsUploadProgress> uploadProgress = null, bool forceLogging = false);
        Task DownloadFile(GcsObject fileObj, string fileLocalPath, Action<CGcsDownloadProgress> downloadProgress = null, bool forceLogging = false);
        Task<string> CreateDownloadUrlSigned(GcsObject fileObj, TimeSpan expiredDurationFromNow);
        Task<string> CreateDownloadUrlSigned(GcsObject fileObj, DateTime expiryDateTime);
        Task<GcsTransferJob> CreateS3ToGcsTransferJob(string s3Bucket, string awsAccessKey, string awsSecretKey, string gcsBucket, string objectPrefix);
        Task<List<GcsTransferJob>> ListTransferJobsAsync();
        Task DeleteTransferJobAsync(string jobName);
        void CleanUp(GcsMediaUpload mediaUpload);
    }

    public sealed class CGoogleCloudStorage : IGoogleCloudStorage {

        private readonly ILogger _logger;
        private readonly IChiper _chiper;
        private readonly IConverter _converter;
        private readonly IBerkas _berkas;

        private string credentialPath = string.Empty;
        private string projectId = string.Empty;

        private GoogleCredential googleCredential = null;
        private StorageService storageService = null;
        private UrlSigner urlSigner = null;
        private StoragetransferService stsService = null;

        public CGoogleCloudStorage(ILogger logger, IChiper chiper, IConverter converter, IBerkas berkas) {
            this._logger = logger;
            this._chiper = chiper;
            this._converter = converter;
            this._berkas = berkas;
        }

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
            _ = json.TryGetValue("project_id", out this.projectId);
            this.googleCredential = GoogleCredential.FromJson(text).CreateScoped(
                StorageService.Scope.DevstorageFullControl,
                StoragetransferService.Scope.CloudPlatform
            );

            using (var ms = new MemoryStream()) {
                using (var writer = new StreamWriter(ms)) {
                    writer.Write(text);
                    writer.Flush();
                    ms.Position = 0;
                    this.urlSigner = UrlSigner.FromServiceAccountData(ms);
                }
            }

            if (!isEncrypted) {
                File.WriteAllText($"{this.credentialPath}.txt", this._chiper.EncryptText(text));
            }
        }

        public void InitializeClient() {
            if (this.googleCredential == null) {
                this.LoadCredential(this.credentialPath, this.credentialPath.ToLower().EndsWith(".txt"));
            }

            var initializer = new BaseClientService.Initializer() {
                HttpClientInitializer = googleCredential,
                ApplicationName = projectId
            };

            this.storageService = new StorageService(initializer);
            this.stsService = new StoragetransferService(initializer);

            this._logger.WriteInfo($"{this.GetType().Name}Client", this.projectId);
        }

        public async Task<List<GcsBucket>> ListAllBuckets() {
            if (this.storageService == null) {
                this.InitializeClient();
            }

            var allBuckets = new List<GcsBucket>();

            BucketsResource.ListRequest request = this.storageService.Buckets.List(this.storageService.ApplicationName);
            request.Fields = "nextPageToken, items";

            ulong pageNum = 1;
            string pageToken = null;
            do {
                this._logger.WriteInfo($"{this.GetType().Name}LoadBucketPage", $"{pageNum}");
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

        public async Task<(List<GcsPrefix>, List<GcsObject>)> ListAllObjects(string bucketName, string prefix = "", string delimiter = "") {
            if (this.storageService == null) {
                this.InitializeClient();
            }

            var resultPrefixes = new List<GcsPrefix>();
            var resultObjects = new List<GcsObject>();

            ObjectsResource.ListRequest request = this.storageService.Objects.List(bucketName);

            request.Fields = "nextPageToken, items, prefixes";

            if (!string.IsNullOrEmpty(prefix)) {
                request.Prefix = prefix;
            }

            if (!string.IsNullOrEmpty(delimiter)) {
                request.Delimiter = delimiter;
            }

            ulong pageNum = 1;
            string pageToken = null;
            do {
                this._logger.WriteInfo($"{this.GetType().Name}LoadObjectPage", $"{pageNum}");

                request.PageToken = pageToken;
                Google.Apis.Storage.v1.Data.Objects objects = await request.ExecuteAsync();
                pageToken = objects.NextPageToken;

                if (objects != null) {
                    if (objects.Prefixes != null) {
                        foreach (string p in objects.Prefixes) {
                            resultPrefixes.Add(new GcsPrefix {
                                BucketName = bucketName,
                                Prefix = p
                            });
                        }
                    }

                    if (objects.Items != null) {
                        foreach (Google.Apis.Storage.v1.Data.Object obj in objects.Items) {
                            if (obj.Name == prefix) {
                                continue;
                            }

                            resultObjects.Add(new GcsObject {
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

            return (resultPrefixes, resultObjects);
        }

        public GcsMediaUpload GenerateUploadMedia(FileInfo fileInfo, string bucketName, string folderName = null) {
            var obj = new GcsObject {
                Name = $"{folderName}{fileInfo.Name}",
                Bucket = bucketName,
                ContentType = this._chiper.GetMime(fileInfo.FullName)
            };

            var mu = new GcsMediaUpload(this.storageService, obj, bucketName, fileInfo.OpenRead(), obj.ContentType) {
                Fields = "id, name, size, contentType",
                ChunkSize = ResumableUpload.MinimumChunkSize
            };

            return mu;
        }

        public GcsMediaUpload GenerateUploadMedia(string fileKey, string bucketName, Stream stream, string contentType, string folderName = null) {
            var obj = new GcsObject {
                Name = $"{folderName}{fileKey}",
                Bucket = bucketName,
                ContentType = contentType
            };

            var mu = new GcsMediaUpload(this.storageService, obj, bucketName, stream, obj.ContentType) {
                Fields = "id, name, size, contentType",
                ChunkSize = ResumableUpload.MinimumChunkSize
            };

            return mu;
        }

        public async Task<Uri> CreateUploadUri(GcsMediaUpload mediaUpload) {
            return await mediaUpload.InitiateSessionAsync();
        }

        public async Task<CGcsUploadProgress> UploadFile(GcsMediaUpload mediaUpload, Uri uploadSession = null, Action<CGcsUploadProgress> uploadProgress = null, bool forceLogging = false) {
            if (uploadSession == null) {
                uploadSession = await this.CreateUploadUri(mediaUpload);
            }

            if (uploadProgress != null) {
                mediaUpload.ProgressChanged += (progressNew) => {
                    _ = Enum.TryParse(progressNew.Status.ToString(), out EGcsUploadStatus progressStatus);
                    var upPrgs = new CGcsUploadProgress {
                        Status = progressStatus,
                        BytesSent = progressNew.BytesSent,
                        Exception = progressNew.Exception
                    };
                    uploadProgress(upPrgs);
                };
            }

            this._logger.WriteInfo($"{this.GetType().Name}UploadStart", $"{mediaUpload.Body.Name} ===>>> {mediaUpload.Bucket} :: {mediaUpload.Body.Size} Bytes", force: forceLogging);
            IUploadProgress result = await mediaUpload.ResumeAsync(uploadSession);
            this._logger.WriteInfo($"{this.GetType().Name}UploadCompleted", $"{mediaUpload.Body.Name} ===>>> {mediaUpload.Bucket} :: 100 %", force: forceLogging);

            _ = Enum.TryParse(result.Status.ToString(), out EGcsUploadStatus uploadStatus);
            return new CGcsUploadProgress {
                Status = uploadStatus,
                BytesSent = result.BytesSent,
                Exception = result.Exception
            };
        }

        public async Task DownloadFile(GcsObject fileObj, string fileLocalPath, Action<CGcsDownloadProgress> downloadProgress = null, bool forceLogging = false) {
            string fileTempPath = Path.Combine(this._berkas.DownloadFolderPath, Path.GetFileName(fileLocalPath));

            long lastDownloadedBytes = 0;
            if (File.Exists(fileTempPath)) {
                lastDownloadedBytes = new FileInfo(fileTempPath).Length;
            }

            if (lastDownloadedBytes >= (long)fileObj.Size) {
                this._logger.WriteInfo($"{this.GetType().Name}DownloadSkipped", $"{fileLocalPath} is already fully downloaded.", force: forceLogging);
                downloadProgress?.Invoke(new CGcsDownloadProgress {
                    Status = EGcsDownloadStatus.Completed,
                    BytesDownloaded = (long)fileObj.Size,
                    Exception = null
                });
                return;
            }

            var doo = new DownloadObjectOptions() {
                ChunkSize = ResumableUpload.MinimumChunkSize,
                Range = new RangeHeaderValue(lastDownloadedBytes, null)
            };

            var idp = new Progress<IDownloadProgress>(progressNew => {
                _ = Enum.TryParse(progressNew.Status.ToString(), out EGcsDownloadStatus progressStatus);
                var dwPrgs = new CGcsDownloadProgress {
                    Status = progressStatus,
                    BytesDownloaded = lastDownloadedBytes + progressNew.BytesDownloaded,
                    Exception = progressNew.Exception
                };
                downloadProgress?.Invoke(dwPrgs);
            });

            StorageClient storage = await StorageClient.CreateAsync(this.googleCredential);

            this._logger.WriteInfo($"{this.GetType().Name}DownloadStart", $"{fileLocalPath} <<<=== {fileObj.Bucket}/{fileObj.Name} :: {fileObj.Size} Bytes", force: forceLogging);

            using (var fs = new FileStream(fileTempPath, FileMode.Append, FileAccess.Write, FileShare.ReadWrite)) {
                await storage.DownloadObjectAsync(fileObj.Bucket, fileObj.Name, fs, doo, progress: idp);
                this._logger.WriteInfo($"{this.GetType().Name}DownloadCompleted", $"{fileLocalPath} <<<=== {fileObj.Bucket}/{fileObj.Name} :: 100 %", force: forceLogging);
            }
        }

        public async Task<string> CreateDownloadUrlSigned(GcsObject fileObj, TimeSpan expiredDurationFromNow) {
            string ddl = await this.urlSigner.SignAsync(fileObj.Bucket, fileObj.Name, expiredDurationFromNow);
            this._logger.WriteInfo($"{this.GetType().Name}DirectDownloadLinkTimeSpan", ddl);
            return ddl;
        }

        public async Task<string> CreateDownloadUrlSigned(GcsObject fileObj, DateTime expiryDateTime) {
            string ddl = await this.urlSigner.SignAsync(fileObj.Bucket, fileObj.Name, expiryDateTime);
            this._logger.WriteInfo($"{this.GetType().Name}DirectDownloadLinkDateTime", ddl);
            return ddl;
        }

        public async Task<GcsTransferJob> CreateS3ToGcsTransferJob(string s3Bucket, string awsAccessKey, string awsSecretKey, string gcsBucket, string objectPrefix) {
            if (this.stsService == null) {
                this.InitializeClient();
            }

            DateTime now = DateTime.UtcNow;

            var excludeList = new List<string>();
            string allPossibleNextChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$%^&*()_+-=[]{}|;':\",./<>?`~";
            foreach (char c in allPossibleNextChars) {
                excludeList.Add(objectPrefix + c);
            }

            var transferJob = new TransferJob {
                Description = $"Transfer {objectPrefix} dari S3 ke GCS via WinForms",
                Status = "ENABLED",
                ProjectId = this.projectId,
                TransferSpec = new TransferSpec {
                    AwsS3DataSource = new AwsS3Data {
                        BucketName = s3Bucket,
                        AwsAccessKey = new AwsAccessKey {
                            AccessKeyId = awsAccessKey,
                            SecretAccessKey = awsSecretKey
                        }
                    },
                    GcsDataSink = new GcsData {
                        BucketName = gcsBucket
                    },
                    ObjectConditions = new ObjectConditions {
                        IncludePrefixes = new List<string> {
                            objectPrefix
                        },
                        ExcludePrefixes = excludeList
                    },
                    TransferOptions = new TransferOptions {
                        OverwriteObjectsAlreadyExistingInSink = false
                    }
                },
                Schedule = new Schedule {
                    ScheduleStartDate = new Date {
                        Year = now.Year,
                        Month = now.Month,
                        Day = now.Day
                    },
                    ScheduleEndDate = new Date {
                        Year = now.Year,
                        Month = now.Month,
                        Day = now.Day
                    }
                }
            };

            this._logger.WriteInfo($"{this.GetType().Name}CreateStsJob", $"Mengirim perintah ke Google untuk {s3Bucket}/{objectPrefix}");

            CreateRequest request = this.stsService.TransferJobs.Create(transferJob);
            TransferJob googleResponse = await request.ExecuteAsync();

            return new GcsTransferJob {
                Name = googleResponse.Name,
                Description = googleResponse.Description,
                Status = googleResponse.Status,
                ProjectId = googleResponse.ProjectId,
                CreationTime = googleResponse.CreationTimeDateTimeOffset?.DateTime
            };
        }

        public async Task<List<GcsTransferJob>> ListTransferJobsAsync() {
            if (this.stsService == null) {
                this.InitializeClient();
            }

            var allJobs = new List<TransferJob>();
            var allOps = new List<Operation>();
            var results = new List<GcsTransferJob>();

            string filterJob = $"{{\"projectId\": \"{this.projectId}\", \"jobStatuses\": [\"ENABLED\", \"DISABLED\"]}}";
            ListRequest requestJob = this.stsService.TransferJobs.List(filterJob);

            string pageTokenJob = null;
            do {
                requestJob.PageToken = pageTokenJob;
                ListTransferJobsResponse responseJob = await requestJob.ExecuteAsync();

                if (responseJob.TransferJobs != null) {
                    allJobs.AddRange(responseJob.TransferJobs);
                }

                pageTokenJob = responseJob.NextPageToken;
            }
            while (pageTokenJob != null);

            string filterOp = $"{{\"projectId\": \"{this.projectId}\"}}";
            TransferOperationsResource.ListRequest requestOp = this.stsService.TransferOperations.List("transferOperations", filterOp);

            string pageTokenOp = null;
            do {
                requestOp.PageToken = pageTokenOp;
                ListOperationsResponse responseOp = await requestOp.ExecuteAsync();

                if (responseOp.Operations != null) {
                    allOps.AddRange(responseOp.Operations);
                }

                pageTokenOp = responseOp.NextPageToken;
            }
            while (pageTokenOp != null);

            foreach (TransferJob job in allJobs) {
                string finalStatus = job.Status;

                Operation myOp = allOps.LastOrDefault(op =>
                    op.Metadata != null &&
                    op.Metadata.ContainsKey("transferJobName") &&
                    op.Metadata["transferJobName"].ToString() == job.Name
                );

                if (myOp != null) {
                    if (myOp.Metadata.ContainsKey("status")) {
                        finalStatus = myOp.Metadata["status"].ToString();
                    }

                    if (finalStatus == "FAILED") {
                        if (myOp.Error != null && !string.IsNullOrEmpty(myOp.Error.Message)) {
                            finalStatus = $"FAILED :: {myOp.Error.Message}";
                        }
                        else if (myOp.Metadata.ContainsKey("errorBreakdowns")) {
                            finalStatus = $"FAILED :: File bermasalah cek di Cloud Console";
                        }
                    }
                }

                results.Add(new GcsTransferJob {
                    Name = job.Name,
                    Description = job.Description,
                    Status = finalStatus,
                    ProjectId = job.ProjectId,
                    CreationTime = job.CreationTimeDateTimeOffset?.DateTime
                });
            }

            return results.OrderByDescending(j => j.CreationTime).ToList();
        }

        public async Task DeleteTransferJobAsync(string jobName) {
            if (this.stsService == null) {
                this.InitializeClient();
            }

            var updateRequest = new UpdateTransferJobRequest {
                ProjectId = this.projectId,
                TransferJob = new TransferJob {
                    Status = "DELETED"
                },
                UpdateTransferJobFieldMask = "status"
            };

            this._logger.WriteInfo($"{this.GetType().Name}DeleteJob", $"Menghapus riwayat job {jobName} dari Google Cloud");

            PatchRequest request = this.stsService.TransferJobs.Patch(updateRequest, jobName);
            _ = await request.ExecuteAsync();
        }

        public void CleanUp(GcsMediaUpload mediaUpload) => mediaUpload.Dispose();

    }

}
