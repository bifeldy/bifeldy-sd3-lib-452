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
using System.Text;

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

        public static void ToCsv(this DbDataReader dr, string delimiter, string outputFilePath = null, bool includeHeader = true, bool useDoubleQuote = true, bool allUppercase = true, Encoding encoding = null) {
            using (var streamWriter = new StreamWriter(outputFilePath, false, encoding ?? Encoding.UTF8)) {
                if (includeHeader) {
                    string header = string.Join(delimiter, Enumerable.Range(0, dr.FieldCount).Select(i => {
                        string text = dr.GetName(i);

                        if (allUppercase) {
                            text = text.ToUpper();
                        }

                        if (useDoubleQuote) {
                            text = $"\"{text.Replace("\"", "\"\"")}\"";
                        }

                        return text;
                    }));

                    streamWriter.WriteLine(header);
                }

                while (dr.Read()) {
                    string line = string.Join(delimiter, Enumerable.Range(0, dr.FieldCount).Select(i => {
                        if (dr.IsDBNull(i)) {
                            return "";
                        }

                        string text = dr.GetValue(i).ToString();

                        if (allUppercase) {
                            text = text.ToUpper();
                        }

                        bool mustQuote = text.Contains(delimiter) || text.Contains('"') || text.Contains('\n') || text.Contains('\r');
                        if (useDoubleQuote || mustQuote) {
                            text = $"\"{text.Replace("\"", "\"\"")}\"";
                        }

                        return text;
                    }));

                    streamWriter.WriteLine(line);
                }
            }
        }

    }

}
