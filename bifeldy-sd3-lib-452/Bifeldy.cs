/**
 * 
 * Author       :: Basilius Bias Astho Christyono
 * Phone        :: (+62) 889 236 6466
 * 
 * Department   :: IT SD 03
 * Mail         :: bias@indomaret.co.id
 * 
 * Catatan      :: Bifeldy's Initial Main Application
 * 
 */

using System;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using System.Threading;

using Autofac;
using Autofac.Builder;

namespace bifeldy_sd3_lib_452 {

    public sealed class Bifeldy {

        // Class Name & Interface Only Start With "Alphabet" And Can Be Combined With Number And Symbol _ Only
        private readonly string _acceptableIndentifierName = "^[a-zA-Z0-9_]+$";

        private readonly ContainerBuilder _builder = null;

        private IContainer container = null;

        public Bifeldy() {
            this._builder = new ContainerBuilder();

            /* Other Solution Project */
            // builder.RegisterAssemblyTypes(Assembly.Load(nameof(nama_project_lain_yang_mau_di_import)))
            //     .Where(vsSln => vsSln.Namespace.Contains("nama_namespace_yang_mau_di_import"))
            //     .As(c => c.GetInterfaces().FirstOrDefault(i => i.Name == "I" + c.Name);

            // Inject CClass As IInterface Using Namespace
            this.RegisterDiClassAsInterfaceByNamespace(Assembly.GetExecutingAssembly(), new string[] {
                "bifeldy_sd3_lib_452.Utilities"
            });

            this.RegisterDiClassAsInterfaceByNamespace(Assembly.GetExecutingAssembly(), new string[] {
                "bifeldy_sd3_lib_452.Databases",
                "bifeldy_sd3_lib_452.Handlers"
            }, false);
        }

        public Bifeldy(string[] args) : this() {
            //            0             1     2     3       4       5
            // bifeldy-sd3-wf-452.exe -arg0 arg1 --arg2 "a r g 3" .....
            for (int i = 0; i < args.Length; i++) {
                Console.WriteLine($"arg[{i}] => {args[i]}");
            }
        }

        /// <summary>Di Panggil Sebelum Resolve();</summary>
        /// <typeparam name="CClass">Nama Class Yang Ingin Di Daftarkan</typeparam>
        /// <typeparam name="IInterface">Nama Interface Dari Class Yang Ingin Di Daftarkan</typeparam>
        /// <param name="singleton">Menggunakan Instance Yang Sama Untuk Keseluruhan Program</param>
        public void RegisterDiClass<CClass>(bool singleton = true) {
            IRegistrationBuilder<
                CClass,
                ConcreteReflectionActivatorData,
                SingleRegistrationStyle
            > registrationBuilder = this._builder
                .RegisterType<CClass>();

            if (singleton) {
                _ = registrationBuilder.SingleInstance();
            }
            else {
                _ = registrationBuilder.InstancePerDependency();
            }
        }

        public void RegisterDiClassByNamespace(Assembly assembly, string[] namespaces, bool singleton = true) {
            IRegistrationBuilder<
                object,
                Autofac.Features.Scanning.ScanningActivatorData,
                DynamicRegistrationStyle
            > registrationBuilder = this._builder
                .RegisterAssemblyTypes(assembly)
                .Where(type => !string.IsNullOrEmpty(type.Namespace) && namespaces.Any(type.Namespace.Contains) && type.IsClass && Regex.IsMatch(type.Name, this._acceptableIndentifierName));

            if (singleton) {
                _ = registrationBuilder.SingleInstance();
            }
            else {
                _ = registrationBuilder.InstancePerDependency();
            }
        }

        public void RegisterDiClassNamed<CClass>(bool singleton = true) {
            IRegistrationBuilder<
                CClass,
                ConcreteReflectionActivatorData,
                SingleRegistrationStyle
            > registrationBuilder = this._builder
                .RegisterType<CClass>()
                .Named<object>(typeof(CClass).Name);

            if (singleton) {
                _ = registrationBuilder.SingleInstance();
            }
            else {
                _ = registrationBuilder.InstancePerDependency();
            }
        }

        public void RegisterDiClassNamedByNamespace(Assembly assembly, string[] namespaces, bool singleton = true) {
            IRegistrationBuilder<
                object,
                Autofac.Features.Scanning.ScanningActivatorData,
                DynamicRegistrationStyle
            > registrationBuilder = this._builder
                .RegisterAssemblyTypes(assembly)
                .Where(type => !string.IsNullOrEmpty(type.Namespace) && namespaces.Any(type.Namespace.Contains) && type.IsClass && Regex.IsMatch(type.Name, this._acceptableIndentifierName))
                .Named<object>(c => c.Name);

            if (singleton) {
                _ = registrationBuilder.SingleInstance();
            }
            else {
                _ = registrationBuilder.InstancePerDependency();
            }
        }

        public void RegisterDiClassAsInterface<CClass, IInterface>(bool singleton = true) {
            IRegistrationBuilder<
                CClass,
                ConcreteReflectionActivatorData,
                SingleRegistrationStyle
            > registrationBuilder = this._builder
                .RegisterType<CClass>().As<IInterface>();

            if (singleton) {
                _ = registrationBuilder.SingleInstance();
            }
            else {
                _ = registrationBuilder.InstancePerDependency();
            }
        }

        public void RegisterDiClassAsInterfaceByNamespace(Assembly assembly, string[] namespaces, bool singleton = true) {
            IRegistrationBuilder<
                object,
                Autofac.Features.Scanning.ScanningActivatorData,
                DynamicRegistrationStyle
            > registrationBuilder = this._builder
                .RegisterAssemblyTypes(assembly)
                .Where(type => !string.IsNullOrEmpty(type.Namespace) && namespaces.Any(type.Namespace.Contains) && type.IsClass && Regex.IsMatch(type.Name, this._acceptableIndentifierName))
                .As(c => c.GetInterfaces().Where(i => i.Name == "I" + c.Name.Substring(1)).First());

            if (singleton) {
                _ = registrationBuilder.SingleInstance();
            }
            else {
                _ = registrationBuilder.InstancePerDependency();
            }
        }

        public IC Resolve<IC>() {
            return this.container.Resolve<IC>();
        }

        public IC ResolveNamed<IC>(string name) {
            return (IC) this.container.ResolveNamed<object>(name);
        }

        public dynamic BeginLifetimeScope() {
            if (this.container == null) {
                this.container = this._builder.Build();
            }

            return this.container.BeginLifetimeScope();
        }

        public void UseInvariantCulture() {
            CultureInfo.DefaultThreadCurrentCulture = CultureInfo.InvariantCulture;
            CultureInfo.DefaultThreadCurrentUICulture = CultureInfo.InvariantCulture;

            Thread.CurrentThread.CurrentCulture = CultureInfo.InvariantCulture;
            Thread.CurrentThread.CurrentUICulture = CultureInfo.InvariantCulture;
        }

    }

}
