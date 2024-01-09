﻿/**
 * 
 * Author       :: Basilius Bias Astho Christyono
 * Phone        :: (+62) 889 236 6466
 * 
 * Department   :: IT SD 03
 * Mail         :: bias@indomaret.co.id
 * 
 * Catatan      :: Rx Pub-Sub
 *              :: Harap Didaftarkan Ke DI Container
 * 
 */

using System;
using System.Collections.Generic;
using System.Dynamic;

using bifeldy_sd3_lib_452.Models;

namespace bifeldy_sd3_lib_452.Utilities {

    public interface IPubSub {
        bool IsExist(string variableName);
        RxBehaviorSubject<T> CreateNewBehaviorSubject<T>(T initialValue);
        RxBehaviorSubject<T> GetGlobalAppBehaviorSubject<T>(string variableName);
        RxBehaviorSubject<T> CreateGlobalAppBehaviorSubject<T>(string variableName, T initialValue);
        void DisposeAndRemoveAllSubscriber(string variableName);
    }

    public sealed class CPubSub : IPubSub {

        private readonly IConverter _converter;

        IDictionary<string, dynamic> keyValuePairs = new ExpandoObject();

        public CPubSub(IConverter converter) {
            _converter = converter;
        }

        public bool IsExist(string variableName) {
            return keyValuePairs.ContainsKey(variableName);
        }

        public RxBehaviorSubject<T> CreateNewBehaviorSubject<T>(T initialValue) {
            return new RxBehaviorSubject<T>(initialValue);
        }

        public RxBehaviorSubject<T> GetGlobalAppBehaviorSubject<T>(string variableName) {
            if (string.IsNullOrEmpty(variableName)) {
                throw new Exception("Nama Variable Wajib Diisi");
            }
            if (!keyValuePairs.ContainsKey(variableName)) {
                T defaultValue = _converter.GetDefaultValueT<T>();
                return CreateGlobalAppBehaviorSubject(variableName, defaultValue);
            }
            return keyValuePairs[variableName];
        }

        public RxBehaviorSubject<T> CreateGlobalAppBehaviorSubject<T>(string variableName, T initialValue) {
            if (string.IsNullOrEmpty(variableName)) {
                throw new Exception("Nama Variable Wajib Diisi");
            }
            if (!keyValuePairs.ContainsKey(variableName)) {
                keyValuePairs.Add(variableName, CreateNewBehaviorSubject(initialValue));
            }
            return keyValuePairs[variableName];
        }

        public void DisposeAndRemoveAllSubscriber(string variableName) {
            if (keyValuePairs.ContainsKey(variableName)) {
                keyValuePairs[variableName].Dispose();
                keyValuePairs.Remove(variableName);
            }
        }

    }

}
