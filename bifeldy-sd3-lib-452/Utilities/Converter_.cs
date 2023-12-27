﻿/**
 * 
 * Author       :: Basilius Bias Astho Christyono
 * Phone        :: (+62) 889 236 6466
 * 
 * Department   :: IT SD 03
 * Mail         :: bias@indomaret.co.id
 * 
 * Catatan      :: Alat Konversi
 *              :: Harap Didaftarkan Ke DI Container
 * 
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.Drawing;
using System.Linq;

using Newtonsoft.Json;

using bifeldy_sd3_lib_452.Extensions;

namespace bifeldy_sd3_lib_452.Utilities {

    public interface IConverter {
        byte[] ImageToByte(Image x);
        Image ByteToImage(byte[] byteArray);
        T JsonToObject<T>(string j2o);
        string ObjectToJson(object body);
        string ByteToString(byte[] bytes, bool removeHypens = true);
        byte[] StringToByte(string hex, string separator = null);
        List<T> DataTableToList<T>(DataTable dt);
        DataTable ListToDataTable<T>(List<T> listData, string tableName = null, string arrayListSingleValueColumnName = null);
        T GetDefaultValueT<T>();
    }

    public sealed class CConverter : IConverter {

        public CConverter() {
            //
        }

        public byte[] ImageToByte(Image image) {
            return (byte[]) new ImageConverter().ConvertTo(image, typeof(byte[]));
        }

        public Image ByteToImage(byte[] byteArray) {
            return (Bitmap) new ImageConverter().ConvertFrom(byteArray);
        }

        public string TimeSpanToEta(TimeSpan ts) {
            return ts.ToEta();
        }

        public T JsonToObject<T>(string j2o) {
            return JsonConvert.DeserializeObject<T>(j2o);
        }

        public string ObjectToJson(object o2j) {
            return JsonConvert.SerializeObject(o2j);
        }

        public string ByteToString(byte[] bytes, bool removeHypens = true) {
            return bytes.ToString(removeHypens);
        }

        public byte[] StringToByte(string hex, string separator = null) {
            return hex.ToByte(separator);
        }

        public List<T> DataTableToList<T>(DataTable dt) {
            return dt.ToList<T>();
        }

        public DataTable ListToDataTable<T>(List<T> listData, string tableName = null, string arrayListSingleValueColumnName = null) {
            return listData.ToDataTable(tableName, arrayListSingleValueColumnName);
        }

        public T GetDefaultValueT<T>() {
            dynamic x = null;
            switch (Type.GetTypeCode(typeof(T))) {
                case TypeCode.DateTime:
                    x = DateTime.MinValue;
                    break;
                case TypeCode.Byte:
                case TypeCode.SByte:
                case TypeCode.Int16:
                case TypeCode.UInt16:
                case TypeCode.Int32:
                case TypeCode.UInt32:
                case TypeCode.Int64:
                case TypeCode.UInt64:
                case TypeCode.Decimal:
                case TypeCode.Double:
                    x = 0;
                    break;
                case TypeCode.Boolean:
                    x = false;
                    break;
            }
            return (T) Convert.ChangeType(x, typeof(T));
        }

    }

}
