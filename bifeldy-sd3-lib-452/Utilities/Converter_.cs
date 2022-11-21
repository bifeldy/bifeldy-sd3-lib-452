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
using System.Data;
using System.Linq;
using System.Reflection;

using Newtonsoft.Json;

namespace bifeldy_sd3_lib_452.Utilities {

    public interface IConverter {
        T JsonToObj<T>(string j2o);
        string ObjectToJson(object body);
        List<T> ConvertDataTableToList<T>(DataTable dt);
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

    }

}
