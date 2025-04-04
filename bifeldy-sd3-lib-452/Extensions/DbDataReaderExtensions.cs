﻿/**
 * 
 * Author       :: Basilius Bias Astho Christyono
 * Phone        :: (+62) 889 236 6466
 * 
 * Department   :: IT SD 03
 * Mail         :: bias@indomaret.co.id
 * 
 * Catatan      :: Tidak Untuk Didaftarkan Ke DI Container
 * 
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Reflection;

namespace bifeldy_sd3_lib_452.Extensions {

    public static class DbDataReaderExtensions {

        public static List<T> ToList<T>(this DbDataReader dr) {
            var ls = new List<T>();
            PropertyInfo[] properties = typeof(T).GetProperties();

            if (dr.HasRows) {
                while (dr.Read()) {
                    var cols = new Dictionary<string, object>();
                    for (int i = 0; i < dr.FieldCount; i++) {
                        if (!dr.IsDBNull(i)) {
                            cols[dr.GetName(i).ToUpper()] = dr.GetValue(i);
                        }
                    }

                    T objT = Activator.CreateInstance<T>();
                    foreach (PropertyInfo pro in properties) {
                        string key = pro.Name.ToUpper();
                        if (cols.ContainsKey(key)) {
                            dynamic val = cols[key];

                            if (val != null) {
                                TypeConverter converter = TypeDescriptor.GetConverter(pro.PropertyType);
                                if (converter.CanConvertFrom(val.GetType())) {
                                    val = converter.ConvertFrom(val);
                                }
                                else {
                                    val = Convert.ChangeType(val, pro.PropertyType);
                                }

                                pro.SetValue(objT, val);
                            }
                        }
                    }

                    ls.Add(objT);
                }
            }

            return ls;
        }

        public static void ToCsv(this DbDataReader dr, string separator, string outputFilePath = null) {
            using (var streamWriter = new StreamWriter(outputFilePath, true)) {
                string struktur = Enumerable.Range(0, dr.FieldCount).Select(i => dr.GetName(i)).Aggregate((i, j) => $"{i}{separator}{j}");
                streamWriter.WriteLine(struktur.ToUpper());
                streamWriter.Flush();

                while (dr.Read()) {
                    var _colValue = new List<string>();
                    for (int i = 0; i < dr.FieldCount; i++) {
                        string val = string.Empty;
                        if (!dr.IsDBNull(i)) {
                            val = dr.GetValue(i).ToString();
                        }

                        _colValue.Add(val);
                    }

                    string line = _colValue.Aggregate((i, j) => $"{i}{separator}{j}");
                    if (!string.IsNullOrEmpty(line)) {
                        streamWriter.WriteLine(line.ToUpper());
                        streamWriter.Flush();
                    }
                }
            }
        }

    }

}
