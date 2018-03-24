using Orleans.Providers;
using Orleans.Runtime;
using System;
using System.Collections.Concurrent;
using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using System.Linq;

namespace Orleans.Storage
{
    public static class GrainStorageExtensions
    {
        private static ConcurrentDictionary<Type, IStorageProvider> _storageProviders = new ConcurrentDictionary<Type, IStorageProvider>();

        /// <summary>
        /// Aquire the storage provider associated with the grain type.
        /// </summary>
        /// <returns></returns>
        public static IStorageProvider GetStorageProvider(this Grain grain, IServiceProvider services)
        {
            var storageProvider = _storageProviders.GetOrAdd(grain.GetType(), type =>
            {
                var attr = type.GetCustomAttributes<StorageProviderAttribute>(true).FirstOrDefault();
                return attr != null
                    ? services.GetServiceByName<IStorageProvider>(attr.ProviderName)
                    : services.GetService<IStorageProvider>();
            });

            if (storageProvider == null)
            {
                grain.ThrowMissingProviderException();
            }

            return storageProvider;
        }

        private static void ThrowMissingProviderException(this Grain grain)
        {
            string errMsg;
            var grainTypeName = grain.GetType().GetParseableName(TypeFormattingOptions.LogFormat);
            var attr = grain.GetType().GetCustomAttributes<StorageProviderAttribute>(true).FirstOrDefault();
            errMsg = string.IsNullOrEmpty(attr?.ProviderName) ?
                $"No default storage provider found loading grain type {grainTypeName}." :
                $"No storage provider named \"{attr.ProviderName}\" found loading grain type {grainTypeName}.";

            throw new BadProviderConfigException(errMsg);
        }
    }
}
