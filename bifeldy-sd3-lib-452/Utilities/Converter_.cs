/**
 * 
 * Author       :: Basilius Bias Astho Christyono
 * Mail         :: bias@indomaret.co.id
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
using System.Linq;
using System.Reflection;

using Newtonsoft.Json;

namespace bifeldy_sd3_lib_452.Utilities {

    public interface IConverter {
        T JsonToObj<T>(string j2o);
        string ObjectToJson(object body);
        List<T> ConvertDataTableToList<T>(DataTable dt);
        DataTable ConvertListToDataTable<T>(string tableName, List<T> listData);
    }

    public sealed class CConverter : IConverter {

        private readonly ILogger _logger;

        public CConverter(ILogger logger) {
            _logger = logger;
        }

        public T JsonToObj<T>(string j2o) {
            return JsonConvert.DeserializeObject<T>(j2o);
        }

        public string ObjectToJson(object o2j) {
            return JsonConvert.SerializeObject(o2j);
        }

        public List<T> ConvertDataTableToList<T>(DataTable dt) {
            List<string> columnNames = dt.Columns.Cast<DataColumn>().Select(c => c.ColumnName.ToLower()).ToList();
            PropertyInfo[] properties = typeof(T).GetProperties();
            return dt.AsEnumerable().Select(row => {
                T objT = Activator.CreateInstance<T>();
                foreach (PropertyInfo pro in properties) {
                    if (columnNames.Contains(pro.Name.ToLower())) {
                        try {
                            pro.SetValue(objT, row[pro.Name]);
                        }
                        catch (Exception ex) {
                            _logger.WriteError(ex, 3);
                        }
                    }
                }
                return objT;
            }).ToList();
        }

        public DataTable ConvertListToDataTable<T>(string tableName, List<T> listData) {
            DataTable table = new DataTable(tableName);

            // Special handling for value types and string
            if (typeof(T).IsValueType || typeof(T).Equals(typeof(string))) {
                DataColumn dc = new DataColumn("Value", typeof(T));
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
                            _logger.WriteError(ex, 3);
                            row[prop.Name] = DBNull.Value;
                        }
                    }
                    table.Rows.Add(row);
                }
            }

            return table;
        }

    }

}
