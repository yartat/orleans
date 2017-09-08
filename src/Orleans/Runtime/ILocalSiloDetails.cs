namespace Orleans.Runtime
{
    /// <summary>
    /// Details of the local silo.
    /// </summary>
    public interface ILocalSiloDetails
    {
        /// <summary>
        /// Gets the name of this silo.
        /// </summary>
        string Name { get; }

        /// <summary>
        /// Gets the address of this silo's inter-silo endpoint.
        /// </summary>
        SiloAddress SiloAddress { get; }

        /// <summary>
        /// Gets the address of this silo's inter-silo endpoint in case silo was started in docker.
        /// </summary>
        SiloAddress HostSiloAddress { get; }
    }
}