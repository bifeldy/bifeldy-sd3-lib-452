/**
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
        bool IsExist(string key);
        RxBehaviorSubject<T> CreateNewBehaviorSubject<T>(T initialValue);
        RxBehaviorSubject<T> GetGlobalAppBehaviorSubject<T>(string key);
        RxBehaviorSubject<T> CreateGlobalAppBehaviorSubject<T>(string key, T initialValue);
        void DisposeAndRemoveSubscriber(string key);
    }

    public sealed class CPubSub : IPubSub {

        private readonly IConverter _converter;
        readonly IDictionary<string, dynamic> keyValuePairs = new ExpandoObject();

        public CPubSub(IConverter converter) {
            this._converter = converter;
        }

        public bool IsExist(string key) {
            return this.keyValuePairs.ContainsKey(key);
        }

        public RxBehaviorSubject<T> CreateNewBehaviorSubject<T>(T initialValue) {
            return new RxBehaviorSubject<T>(initialValue);
        }

        public RxBehaviorSubject<T> GetGlobalAppBehaviorSubject<T>(string key) {
            return string.IsNullOrEmpty(key)
                ? throw new Exception("Nama Key Wajib Diisi")
                : !this.keyValuePairs.ContainsKey(key) ? this.CreateGlobalAppBehaviorSubject(key, default(T)) : (RxBehaviorSubject<T>) this.keyValuePairs[key];
        }

        public RxBehaviorSubject<T> CreateGlobalAppBehaviorSubject<T>(string key, T initialValue) {
            if (string.IsNullOrEmpty(key)) {
                throw new Exception("Nama Key Wajib Diisi");
            }

            if (!this.keyValuePairs.ContainsKey(key)) {
                this.keyValuePairs.Add(key, this.CreateNewBehaviorSubject(initialValue));
            }

            return this.keyValuePairs[key];
        }

        public void DisposeAndRemoveSubscriber(string key) {
            if (this.keyValuePairs.ContainsKey(key)) {
                this.keyValuePairs[key].Dispose();
                _ = this.keyValuePairs.Remove(key);
            }
        }

    }

}
