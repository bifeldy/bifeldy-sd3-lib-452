/**
* 
* Author       :: Basilius Bias Astho Christyono
* Phone        :: (+62) 889 236 6466
* 
* Department   :: IT SD 03
* Mail         :: bias@indomaret.co.id
* 
* Catatan      :: Template Gcs Bucket & Object
*              :: Karena Ambigu Dengan Tipe Data C#
*              :: Model Supaya Tidak Perlu Install Package Nuget Google Drive
* 
*/

using System.IO;

using Google.Apis.Download;
using Google.Apis.Services;
using Google.Apis.Upload;

using static Google.Apis.Storage.v1.ObjectsResource;

namespace bifeldy_sd3_lib_452.Models {

    public interface IGcsUploadProgress : IUploadProgress {
        //
    }

    public interface IGcsDownloadProgress : IDownloadProgress {
        //
    }

    public interface IGcsClientService : IClientService {
        //
    }

    public sealed class GcsBucket : Google.Apis.Storage.v1.Data.Bucket {
        //
    }

    public sealed class GcsObject : Google.Apis.Storage.v1.Data.Object {
        //
    }

    public sealed class GcsMediaUpload : InsertMediaUpload {

        public GcsMediaUpload(IGcsClientService clientService, GcsObject body, string bucket, Stream stream, string contentType) : base(clientService, body, bucket, stream, contentType) {
            //
        }

    }

}
