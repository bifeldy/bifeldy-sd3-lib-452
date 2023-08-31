/**
 * 
 * Author       :: Basilius Bias Astho Christyono
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
        string GZipDecompressString(byte[] byteData, int maxChunk = 2048);
        byte[] GZipCompressString(string text);
        MemoryStream ReadFileAsBinaryByte(string filePath, int maxChunk = 2048);
    }

    public sealed class CStream : IStream {

        public CStream() {
            //
        }

        public string GZipDecompressString(byte[] byteData, int maxChunk = 2048) {
            byte[] buffer = new byte[maxChunk - 1];
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

        public byte[] GZipCompressString(string text) {
            byte[] tempByte = Encoding.UTF8.GetBytes(text);
            MemoryStream memory = new MemoryStream();
            GZipStream gzip = new GZipStream(memory, CompressionMode.Compress, true);
            gzip.Write(tempByte, 0, tempByte.Length);
            return memory.ToArray();
        }

        public MemoryStream ReadFileAsBinaryByte(string filePath, int maxChunk = 2048) {
            MemoryStream dest = new MemoryStream();
            using (Stream source = File.OpenRead(filePath)) {
                byte[] buffer = new byte[maxChunk];
                int bytesRead = 0;
                while ((bytesRead = source.Read(buffer, 0, buffer.Length)) > 0) {
                    dest.Write(buffer, 0, bytesRead);
                }
            }
            return dest;
        }

    }

}
