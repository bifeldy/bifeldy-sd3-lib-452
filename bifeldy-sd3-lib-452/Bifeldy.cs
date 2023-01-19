﻿/**
 * 
 * Author       :: Basilius Bias Astho Christyono
 * Mail         :: bias@indomaret.co.id
 * Phone        :: (+62) 889 236 6466
 * 
 * Department   :: IT SD 03
 * Mail         :: bias@indomaret.co.id
 * 
 * Catatan      :: Bifeldy's Initial Main Application
 * 
 */

using System;
using System.Linq;
using System.Reflection;

using Autofac;
using Autofac.Builder;

namespace bifeldy_sd3_lib_452 {

    public class Bifeldy {

        private readonly ContainerBuilder _builder = null;

        private IContainer container = null;

        public Bifeldy() {
            _builder = new ContainerBuilder();

            /* Other Solution Project */
            // builder.RegisterAssemblyTypes(Assembly.Load(nameof(nama_project_lain_yang_mau_di_import)))
            //     .Where(vsSln => vsSln.Namespace.Contains("nama_namespace_yang_mau_di_import"))
            //     .As(c => c.GetInterfaces().FirstOrDefault(i => i.Name == "I" + c.Name);

            // Array Of Namespace For DI
            string[] namespaceForDependencyInjection = { "Databases", "Utilities" };

            // Inject CClass As IInterface
            RegisterDiClassAsInterfaceByNamespace(Assembly.GetExecutingAssembly(), namespaceForDependencyInjection);
        }

        public Bifeldy(string[] args) : this() {
            // bifeldy-sd3-wf-452.exe -arg0 arg1 --arg2 "a r g 3"
            for (int i = 0; i < args.Length; i++) {
                Console.WriteLine($"arg[{i}] => {args[i]}");
            }
        }

        /// <summary>Di Panggil Sebelum ResolveClass();</summary>
        /// <typeparam name="CClass">Nama Class Yang Ingin Di Daftarkan</typeparam>
        /// <typeparam name="IInterface">Nama Interface Dari Class Yang Ingin Di Daftarkan</typeparam>
        /// <param name="singleton">Menggunakan Instance Yang Sama Untuk Keseluruhan Program</param>
        public void RegisterDiClass<CClass>(bool singleton = true) {
            IRegistrationBuilder<
                CClass,
                ConcreteReflectionActivatorData,
                SingleRegistrationStyle
            > registrationBuilder = _builder.RegisterType<CClass>();
            if (singleton) {
                registrationBuilder.SingleInstance();
            }
        }

        public void RegisterDiClassByNamespace(Assembly assembly, string[] namespaces, bool singleton = true) {
            IRegistrationBuilder<
                object,
                Autofac.Features.Scanning.ScanningActivatorData,
                DynamicRegistrationStyle
            > registrationBuilder = _builder
                                        .RegisterAssemblyTypes(assembly)
                                        .Where(type => namespaces.Any(type.Namespace.Contains) && type.IsClass);
            if (singleton) {
                registrationBuilder.SingleInstance();
            }
        }

        public void RegisterDiClassNamed<CClass>(bool singleton = true) {
            IRegistrationBuilder<
                CClass,
                ConcreteReflectionActivatorData,
                SingleRegistrationStyle
            > registrationBuilder = _builder.RegisterType<CClass>().Named<object>(typeof(CClass).Name);
            if (singleton) {
                registrationBuilder.SingleInstance();
            }
        }

        public void RegisterDiClassNamedByNamespace(Assembly assembly, string[] namespaces, bool singleton = true) {
            IRegistrationBuilder<
                object,
                Autofac.Features.Scanning.ScanningActivatorData,
                DynamicRegistrationStyle
            > registrationBuilder = _builder
                                        .RegisterAssemblyTypes(assembly)
                                        .Where(type => namespaces.Any(type.Namespace.Contains) && type.IsClass)
                                        .Named<object>(c => c.Name);
            if (singleton) {
                registrationBuilder.SingleInstance();
            }
        }

        public void RegisterDiClassAsInterface<CClass, IInterface>(bool singleton = true) {
            IRegistrationBuilder<
                CClass,
                ConcreteReflectionActivatorData,
                SingleRegistrationStyle
            > registrationBuilder = _builder.RegisterType<CClass>().As<IInterface>();
            if (singleton) {
                registrationBuilder.SingleInstance();
            }
        }

        public void RegisterDiClassAsInterfaceByNamespace(Assembly assembly, string[] namespaces, bool singleton = true) {
            IRegistrationBuilder<
                object,
                Autofac.Features.Scanning.ScanningActivatorData,
                DynamicRegistrationStyle
            > registrationBuilder = _builder
                                        .RegisterAssemblyTypes(assembly)
                                        .Where(type => namespaces.Any(type.Namespace.Contains) && type.IsClass)
                                        .As(c => c.GetInterfaces().FirstOrDefault(i => i.Name == "I" + c.Name.Substring(1)));
            if (singleton) {
                registrationBuilder.SingleInstance();
            }
        }

        public CClass ResolveClass<CClass>() {
            return container.Resolve<CClass>();
        }

        public object ResolveNamed(string name) {
            return container.ResolveNamed<object>(name);
        }

        public ILifetimeScope BeginLifetimeScope() {
            if (container == null) {
                container = _builder.Build();
            }
            return container.BeginLifetimeScope();
        }

    }

}
