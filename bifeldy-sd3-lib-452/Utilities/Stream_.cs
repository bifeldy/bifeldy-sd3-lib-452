/**
 * 
 * Author       :: Basilius Bias Astho Christyono
 * Mail         :: bias@indomaret.co.id
 * Phone        :: (+62) 889 236 6466
 * 
 * Department   :: IT SD 03
 * Mail         :: bias@indomaret.co.id
 * 
 * Catatan      :: Stream Tools
 *              :: Harap Didaftarkan Ke DI Container
 * 
 */

using System;
using System.IO;
using System.IO.Compression;
using System.Text;

namespace bifeldy_sd3_lib_452.Utilities {

    public interface IStream {
        string DecompressG(byte[] byteData);
        byte[] MemStream(string jString);
    }

    public sealed class CStream : IStream {

        public CStream() {
            //
        }

        public string DecompressG(byte[] byteData) {
            int size = 4096;
            byte[] buffer = new byte[size - 1];
            GZipStream stream = new GZipStream(new MemoryStream(byteData), CompressionMode.Decompress);
            MemoryStream memoryStream = new MemoryStream();
            int count = 0;
            do {
                count = stream.Read(buffer, 0, count);
                if (count > 0) {
                    memoryStream.Write(buffer, 0, count);
                }
            }
            while (count > 0);
            return Encoding.UTF8.GetString(memoryStream.ToArray());
        }

        public byte[] MemStream(string jString) {
            byte[] tempByte = Encoding.UTF8.GetBytes(jString);
            MemoryStream memory = new MemoryStream();
            GZipStream gzip = new GZipStream(memory, CompressionMode.Compress, true);
            gzip.Write(tempByte, 0, tempByte.Length);
            return memory.ToArray();
        }

    }

}
