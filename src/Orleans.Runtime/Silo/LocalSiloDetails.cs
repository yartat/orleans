using System;
using System.Net;
using System.Net.Sockets;
using Microsoft.Extensions.Options;
using Orleans.Hosting;
using Orleans.Runtime.Configuration;

namespace Orleans.Runtime
{
    internal class LocalSiloDetails : ILocalSiloDetails
    {
        private readonly Lazy<SiloAddress> siloAddressLazy;
        private readonly Lazy<SiloAddress> siloHostAddressLazy;
        private readonly Lazy<SiloAddress> gatewayAddressLazy;

        public LocalSiloDetails(
            IOptions<SiloOptions> siloOptions,
            IOptions<EndpointOptions> siloEndpointOptions)
        {
            var options = siloOptions.Value;
            this.Name = options.SiloName;
            this.ClusterId = options.ClusterId;
            this.DnsHostName = Dns.GetHostName();

            var endpointOptions = siloEndpointOptions.Value;
            this.siloAddressLazy = new Lazy<SiloAddress>(() => SiloAddress.New(ResolveEndpoint(endpointOptions), SiloAddress.AllocateNewGeneration()));
            this.siloHostAddressLazy = new Lazy<SiloAddress>(() => SiloAddress.New(ResolveHostEndpoint(endpointOptions), SiloAddress.AllocateNewGeneration()));
            this.gatewayAddressLazy = new Lazy<SiloAddress>(() => endpointOptions.ProxyPort != 0 ? SiloAddress.New(new IPEndPoint((this.HostSiloAddress ?? this.SiloAddress).Endpoint.Address, endpointOptions.ProxyPort), 0) : null);
        }

        private static IPEndPoint ResolveEndpoint(EndpointOptions options)
        {
            IPAddress ipAddress;
            if (options.IPAddress != null)
            {
                ipAddress = options.IPAddress;
            }
            else
            {
                // TODO: refactor this out of ClusterConfiguration
                ipAddress = ClusterConfiguration.ResolveIPAddress(options.HostNameOrIPAddress, null, AddressFamily.InterNetwork).GetAwaiter().GetResult();
            }

            return new IPEndPoint(ipAddress, options.Port);
        }

        private static IPEndPoint ResolveHostEndpoint(EndpointOptions options)
        {
            if (options.HostIPAddress != null)
            {
                return new IPEndPoint(options.HostIPAddress, options.Port);
            }

            return null;
        }

        /// <inheritdoc />
        public string Name { get; }

        /// <inheritdoc />
        public string ClusterId { get; }

        /// <inheritdoc />
        public string DnsHostName { get; }

        /// <inheritdoc />
        public SiloAddress SiloAddress => this.siloAddressLazy.Value;

        /// <inheritdoc />
        public SiloAddress HostSiloAddress => this.siloHostAddressLazy.Value;

        /// <inheritdoc />
        public SiloAddress GatewayAddress => this.gatewayAddressLazy.Value;
    }
}