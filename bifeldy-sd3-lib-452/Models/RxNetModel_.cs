/**
* 
* Author       :: Basilius Bias Astho Christyono
* Phone        :: (+62) 889 236 6466
* 
* Department   :: IT SD 03
* Mail         :: bias@indomaret.co.id
* 
* Catatan      :: Template Behavior Subject
*              :: Model Supaya Tidak Perlu Install Package Nuget RxNET
* 
*/

using System;

using System.Reactive.Subjects;

namespace bifeldy_sd3_lib_452.Models {

    // Wrapper Saja ~ Gak Bisa Inherit Langsung Dari
    // sealed BehaviorSubject

    public sealed class RxBehaviorSubject<T> : SubjectBase<T>, IDisposable {
        readonly BehaviorSubject<T> _backing = null;

        public RxBehaviorSubject(T value) {
            this._backing = new BehaviorSubject<T>(value);
        }

        public override bool HasObservers => this._backing.HasObservers;

        public override bool IsDisposed => this._backing.IsDisposed;

        public T Value => this._backing.Value;

        public bool TryGetValue(out T value) {
            return this._backing.TryGetValue(out value);
        }

        public override void Dispose() {
            this._backing.Dispose();
        }

        public override void OnCompleted() {
            this._backing.OnCompleted();
        }

        public override void OnError(Exception error) {
            this._backing.OnError(error);
        }

        public override void OnNext(T value) {
            this._backing.OnNext(value);
        }

        public override IDisposable Subscribe(IObserver<T> observer) {
            return this._backing.Subscribe(observer);
        }

    }

}
