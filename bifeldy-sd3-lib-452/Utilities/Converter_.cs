/**
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
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Reflection;

using Newtonsoft.Json;

namespace bifeldy_sd3_lib_452.Utilities {

    public interface IConverter {
        byte[] ImageToByte(Image x);
        Image ByteToImage(byte[] byteArray);
        T JsonToObj<T>(string j2o);
        string ObjectToJson(object body);
        string ByteToString(byte[] bytes, bool removeHypens = true);
        byte[] StringToByte(string hex, string separator = null);
        List<T> DataTableToList<T>(DataTable dt);
        DataTable ListToDataTable<T>(List<T> listData, string tableName = null, string arrayListSingleValueColumnName = null);
        T GetDefaultValueT<T>();

    }

    public sealed class CConverter : IConverter {

        private readonly ILogger _logger;

        public CConverter(ILogger logger) {
            _logger = logger;
        }

        public byte[] ImageToByte(Image image) {
            return (byte[]) new ImageConverter().ConvertTo(image, typeof(byte[]));
        }

        public Image ByteToImage(byte[] byteArray) {
            return (Bitmap) new ImageConverter().ConvertFrom(byteArray);
        }

        public T JsonToObj<T>(string j2o) {
            return JsonConvert.DeserializeObject<T>(j2o);
        }

        public string ObjectToJson(object o2j) {
            return JsonConvert.SerializeObject(o2j);
        }

        public string ByteToString(byte[] bytes, bool removeHypens = true) {
            string hex = BitConverter.ToString(bytes);
            if (removeHypens) {
                return hex.Replace("-", "");
            }
            return hex;
        }

        public byte[] StringToByte(string hex, string separator = null) {
            byte[] array = null;
            if (string.IsNullOrEmpty(separator)) {
                int length = (hex.Length + 1) / 3;
                array = new byte[length];
                for (int i = 0; i < length; i++) {
                    array[i] = Convert.ToByte(hex.Substring(3 * i, 2), 16);
                }
            }
            else {
                string[] arr = hex.Split('-');
                array = new byte[arr.Length];
                for (int i = 0; i < arr.Length; i++) {
                    array[i] = Convert.ToByte(arr[i], 16);
                }
            }
            return array;
        }

        public List<T> DataTableToList<T>(DataTable dt) {
            List<string> columnNames = dt.Columns.Cast<DataColumn>().Select(c => c.ColumnName.ToUpper()).ToList();
            PropertyInfo[] properties = typeof(T).GetProperties();

            return dt.AsEnumerable().Select(row => {
                T objT = Activator.CreateInstance<T>();
                foreach (PropertyInfo pro in properties) {
                    if (columnNames.Contains(pro.Name.ToUpper())) {
                        try {
                            pro.SetValue(objT, row[pro.Name]);
                        }
                        catch (Exception ex) {
                            // _logger.WriteError(ex, 3);
                        }
                    }
                }
                return objT;
            }).ToList();
        }

        public DataTable ListToDataTable<T>(List<T> listData, string tableName = null, string arrayListSingleValueColumnName = null) {
            if (string.IsNullOrEmpty(tableName)) {
                tableName = typeof(T).Name;
            }
            DataTable table = new DataTable(tableName);

            //
            // Special handling for value types and string
            //
            // Tabel hanya punya 1 kolom
            // create table `tblNm` ( `tblCl` varchar(255) );
            //
            // List<string> ls = new List<string> { "Row1", "Row2", "Row3" };
            // ListToDataTable(ls, "tblNm", "tblCl");
            //
            if (typeof(T).IsValueType || typeof(T).Equals(typeof(string))) {
                if (string.IsNullOrEmpty(arrayListSingleValueColumnName)) {
                    throw new Exception("Nama Kolom Tabel Wajib Di Isi");
                }

                DataColumn dc = new DataColumn(arrayListSingleValueColumnName, typeof(T));
                table.Columns.Add(dc);
                foreach (T item in listData) {
                    DataRow dr = table.NewRow();
                    dr[0] = item;
                    table.Rows.Add(dr);
                }
            }
            else {
                PropertyDescriptorCollection properties = TypeDescriptor.GetProperties(typeof(T));
                foreach (PropertyDescriptor prop in properties) {
                    table.Columns.Add(prop.Name, Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType);
                }

                foreach (T item in listData) {
                    DataRow row = table.NewRow();
                    foreach (PropertyDescriptor prop in properties) {
                        try {
                            row[prop.Name] = prop.GetValue(item) ?? DBNull.Value;
                        }
                        catch (Exception ex) {
                            // _logger.WriteError(ex, 3);
                            row[prop.Name] = DBNull.Value;
                        }
                    }
                    table.Rows.Add(row);
                }
            }

            return table;
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
