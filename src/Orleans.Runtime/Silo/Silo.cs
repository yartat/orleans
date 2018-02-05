using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Runtime;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.CodeGeneration;
using Orleans.Core;
using Orleans.Hosting;
using Orleans.Runtime.Configuration;
using Orleans.Runtime.ConsistentRing;
using Orleans.Runtime.Counters;
using Orleans.Runtime.GrainDirectory;
using Orleans.Runtime.LogConsistency;
using Orleans.Runtime.Messaging;
using Orleans.Runtime.MultiClusterNetwork;
using Orleans.Runtime.Providers;
using Orleans.Runtime.Scheduler;
using Orleans.Services;
using Orleans.Streams;
using Orleans.Transactions;
using Orleans.Runtime.Versions;
using Orleans.Versions;
using Orleans.ApplicationParts;
using Orleans.Serialization;

namespace Orleans.Runtime
{
    /// <summary>
    /// Orleans silo.
    /// </summary>
    public class Silo
    {
        /// <summary> Standard name for Primary silo. </summary>
        public const string PrimarySiloName = "Primary";

        /// <summary> Silo Types. </summary>
        public enum SiloType
        {
            /// <summary> No silo type specified. </summary>
            None = 0,
            /// <summary> Primary silo. </summary>
            Primary,
            /// <summary> Secondary silo. </summary>
            Secondary,
        }

        private readonly ILocalSiloDetails siloDetails;
        private readonly SiloOptions siloOptions;
        private readonly ISiloMessageCenter messageCenter;
        private readonly OrleansTaskScheduler scheduler;
        private readonly LocalGrainDirectory localGrainDirectory;
        private readonly ActivationDirectory activationDirectory;
        private readonly IncomingMessageAgent incomingAgent;
        private readonly IncomingMessageAgent incomingSystemAgent;
        private readonly IncomingMessageAgent incomingPingAgent;
        private readonly ILogger logger;
        private TypeManager typeManager;
        private readonly TaskCompletionSource<int> siloTerminatedTask =
            new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly SiloStatisticsManager siloStatistics;
        private readonly InsideRuntimeClient runtimeClient;
        private IReminderService reminderService;
        private SystemTarget fallbackScheduler;
        private readonly IMembershipOracle membershipOracle;
        private readonly IMultiClusterOracle multiClusterOracle;
        private readonly ExecutorService executorService;
        private Watchdog platformWatchdog;
        private readonly TimeSpan initTimeout;
        private readonly TimeSpan stopTimeout = TimeSpan.FromMinutes(1);
        private readonly Catalog catalog;
        private readonly List<IHealthCheckParticipant> healthCheckParticipants = new List<IHealthCheckParticipant>();
        private readonly object lockable = new object();
        private readonly GrainFactory grainFactory;
        private readonly IGrainRuntime grainRuntime;
        private readonly ILifecycleObserver siloLifecycle;

        private readonly ILoggerFactory loggerFactory;
        /// <summary>
        /// Gets the type of this 
        /// </summary>
        internal string Name => this.siloDetails.Name;
        internal OrleansTaskScheduler LocalScheduler { get { return this.scheduler; } }
        internal ILocalGrainDirectory LocalGrainDirectory { get { return this.localGrainDirectory; } }
        internal IMultiClusterOracle LocalMultiClusterOracle { get { return this.multiClusterOracle; } }
        internal IConsistentRingProvider RingProvider { get; private set; }
        internal ISiloPerformanceMetrics Metrics { get { return this.siloStatistics.MetricsTable; } }
        internal ICatalog Catalog => this.catalog;

        internal SystemStatus SystemStatus { get; set; }

        internal IServiceProvider Services { get; }

        /// <summary> SiloAddress for this silo. </summary>
        public SiloAddress SiloAddress => this.siloDetails.SiloAddress;
        public SiloAddress SiloHostAddress => this.siloDetails.HostSiloAddress;

        /// <summary>
        ///  Silo termination event used to signal shutdown of this silo.
        /// </summary>
        public WaitHandle SiloTerminatedEvent // one event for all types of termination (shutdown, stop and fast kill).
            => ((IAsyncResult)this.siloTerminatedTask.Task).AsyncWaitHandle;

        public Task SiloTerminated { get { return this.siloTerminatedTask.Task; } } // one event for all types of termination (shutdown, stop and fast kill).

        private SchedulingContext membershipOracleContext;
        private SchedulingContext multiClusterOracleContext;
        private SchedulingContext reminderServiceContext;

        /// <summary>
        /// Initializes a new instance of the <see cref="Silo"/> class.
        /// </summary>
        /// <param name="initializationParams">The silo initialization parameters.</param>
        /// <param name="services">Dependency Injection container</param>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope",
            Justification = "Should not Dispose of messageCenter in this method because it continues to run / exist after this point.")]
        public Silo(ILocalSiloDetails siloDetails, IServiceProvider services)
        {
            string name = siloDetails.Name;
            // Temporarily still require this. Hopefuly gone when 2.0 is released.
            this.siloDetails = siloDetails;
            this.SystemStatus = SystemStatus.Creating;
            AsynchAgent.IsStarting = true;

            var startTime = DateTime.UtcNow;

            IOptions<SiloStatisticsOptions> statisticsOptions = services.GetRequiredService<IOptions<SiloStatisticsOptions>>();
            StatisticsCollector.Initialize(statisticsOptions.Value.CollectionLevel);

            IOptions<MembershipOptions> membershipOptions = services.GetRequiredService<IOptions<MembershipOptions>>();
            this.initTimeout = membershipOptions.Value.MaxJoinAttemptTime;
            if (Debugger.IsAttached)
            {
                this.initTimeout = StandardExtensions.Max(TimeSpan.FromMinutes(10), membershipOptions.Value.MaxJoinAttemptTime);
                this.stopTimeout = this.initTimeout;
            }

            var localEndpoint = this.siloDetails.SiloAddress.Endpoint;

            services.GetService<SerializationManager>().RegisterSerializers(services.GetService<IApplicationPartManager>());

            this.Services = services;
            this.Services.InitializeSiloUnobservedExceptionsHandler();
            //set PropagateActivityId flag from node config
            IOptions<SiloMessagingOptions> messagingOptions = services.GetRequiredService<IOptions<SiloMessagingOptions>>();
            RequestContext.PropagateActivityId = messagingOptions.Value.PropagateActivityId;
            this.loggerFactory = this.Services.GetRequiredService<ILoggerFactory>();
            this.logger = this.loggerFactory.CreateLogger<Silo>();

            this.logger.Info(ErrorCode.SiloGcSetting, "Silo starting with GC settings: ServerGC={0} GCLatencyMode={1}", GCSettings.IsServerGC, Enum.GetName(typeof(GCLatencyMode), GCSettings.LatencyMode));
            if (!GCSettings.IsServerGC)
            {
                this.logger.Warn(ErrorCode.SiloGcWarning, "Note: Silo not running with ServerGC turned on - recommend checking app config : <configuration>-<runtime>-<gcServer enabled=\"true\">");
                this.logger.Warn(ErrorCode.SiloGcWarning, "Note: ServerGC only kicks in on multi-core systems (settings enabling ServerGC have no effect on single-core machines).");
            }

            this.logger.Info(ErrorCode.SiloInitializing, "-------------- Initializing silo on host {0} MachineName {1} at {2}, gen {3} --------------",
                this.siloDetails.DnsHostName, Environment.MachineName, localEndpoint, this.siloDetails.SiloAddress.Generation);
            this.logger.Info(ErrorCode.SiloInitConfig, "Starting silo {0}", name);

            var siloMessagingOptions = this.Services.GetRequiredService<IOptions<SiloMessagingOptions>>();
            BufferPool.InitGlobalBufferPool(siloMessagingOptions.Value);

            try
            {
                this.grainFactory = this.Services.GetRequiredService<GrainFactory>();
            }
            catch (InvalidOperationException exc)
            {
                this.logger.Error(ErrorCode.SiloStartError, "Exception during Silo.Start, GrainFactory was not registered in Dependency Injection container", exc);
                throw;
            }

            // Performance metrics
            this.siloStatistics = this.Services.GetRequiredService<SiloStatisticsManager>();

            // The scheduler
            this.scheduler = this.Services.GetRequiredService<OrleansTaskScheduler>();
            this.healthCheckParticipants.Add(this.scheduler);

            this.runtimeClient = this.Services.GetRequiredService<InsideRuntimeClient>();

            // Initialize the message center
            this.messageCenter = this.Services.GetRequiredService<MessageCenter>();
            var dispatcher = this.Services.GetRequiredService<Dispatcher>();
            this.messageCenter.RerouteHandler = dispatcher.RerouteMessage;
            this.messageCenter.SniffIncomingMessage = this.runtimeClient.SniffIncomingMessage;

            // GrainRuntime can be created only here, after messageCenter was created.
            this.grainRuntime = this.Services.GetRequiredService<IGrainRuntime>();

            // Now the router/directory service
            // This has to come after the message center //; note that it then gets injected back into the message center.;
            this.localGrainDirectory = this.Services.GetRequiredService<LocalGrainDirectory>();

            // Now the activation directory.
            this.activationDirectory = this.Services.GetRequiredService<ActivationDirectory>();

            // Now the consistent ring provider
            this.RingProvider = this.Services.GetRequiredService<IConsistentRingProvider>();

            this.catalog = this.Services.GetRequiredService<Catalog>();
            this.siloStatistics.MetricsTable.Scheduler = this.scheduler;
            this.siloStatistics.MetricsTable.ActivationDirectory = this.activationDirectory;
            this.siloStatistics.MetricsTable.ActivationCollector = this.catalog.ActivationCollector;
            this.siloStatistics.MetricsTable.MessageCenter = this.messageCenter;

            this.executorService = this.Services.GetRequiredService<ExecutorService>();

            // Now the incoming message agents
            var messageFactory = this.Services.GetRequiredService<MessageFactory>();
            this.incomingSystemAgent = new IncomingMessageAgent(Message.Categories.System, this.messageCenter, this.activationDirectory, this.scheduler, this.catalog.Dispatcher, messageFactory, this.executorService, this.loggerFactory);
            this.incomingPingAgent = new IncomingMessageAgent(Message.Categories.Ping, this.messageCenter, this.activationDirectory, this.scheduler, this.catalog.Dispatcher, messageFactory, this.executorService, this.loggerFactory);
            this.incomingAgent = new IncomingMessageAgent(Message.Categories.Application, this.messageCenter, this.activationDirectory, this.scheduler, this.catalog.Dispatcher, messageFactory, this.executorService, this.loggerFactory);

            this.membershipOracle = this.Services.GetRequiredService<IMembershipOracle>();
            this.siloOptions = this.Services.GetRequiredService<IOptions<SiloOptions>>().Value;
            var multiClusterOptions = this.Services.GetRequiredService<IOptions<MultiClusterOptions>>().Value;

            if (!multiClusterOptions.HasMultiClusterNetwork)
            {
                this.logger.Info("Skip multicluster oracle creation (no multicluster network configured)");
            }
            else
            {
                this.multiClusterOracle = this.Services.GetRequiredService<IMultiClusterOracle>();
            }

            this.SystemStatus = SystemStatus.Created;
            AsynchAgent.IsStarting = false;

            StringValueStatistic.FindOrCreate(StatisticNames.SILO_START_TIME,
                () => LogFormatter.PrintDate(startTime)); // this will help troubleshoot production deployment when looking at MDS logs.

            var fullSiloLifecycle = this.Services.GetRequiredService<SiloLifecycle>();
            this.siloLifecycle = fullSiloLifecycle;
            // register all lifecycle participants
            IEnumerable<ILifecycleParticipant<ISiloLifecycle>> lifecycleParticipants = this.Services.GetServices<ILifecycleParticipant<ISiloLifecycle>>();
            foreach(ILifecycleParticipant<ISiloLifecycle> participant in lifecycleParticipants)
            {
                participant?.Participate(fullSiloLifecycle);
            }
            // register all named lifecycle participants
            IKeyedServiceCollection<string, ILifecycleParticipant<ISiloLifecycle>> namedLifecycleParticipantCollection = this.Services.GetService<IKeyedServiceCollection<string,ILifecycleParticipant<ISiloLifecycle>>>();
            foreach (ILifecycleParticipant<ISiloLifecycle> participant in namedLifecycleParticipantCollection
                ?.GetServices(this.Services)
                ?.Select(s => s.GetService(this.Services)))
            {
                participant?.Participate(fullSiloLifecycle);
            }

            // add self to lifecycle
            this.Participate(fullSiloLifecycle);

            this.logger.Info(ErrorCode.SiloInitializingFinished, "-------------- Started silo {0}, ConsistentHashCode {1:X} --------------", this.SiloAddress.ToLongString(), this.SiloAddress.GetConsistentHashCode());
        }

        public void Start()
        {
            StartAsync(CancellationToken.None).GetAwaiter().GetResult();
        }

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            try
            {
                await this.siloLifecycle.OnStart(cancellationToken);
            }
            catch (Exception exc)
            {
                this.logger.Error(ErrorCode.SiloStartError, "Exception during Silo.Start", exc);
                throw;
            }
        }

        private void CreateSystemTargets()
        {
            this.logger.Debug("Creating System Targets for this silo.");

            this.logger.Debug("Creating {0} System Target", "SiloControl");
            var siloControl = ActivatorUtilities.CreateInstance<SiloControl>(this.Services);
            RegisterSystemTarget(siloControl);

            this.logger.Debug("Creating {0} System Target", "ProtocolGateway");
            RegisterSystemTarget(new ProtocolGateway(this.SiloAddress, this.SiloHostAddress, this.loggerFactory));

            this.logger.Debug("Creating {0} System Target", "DeploymentLoadPublisher");
            RegisterSystemTarget(this.Services.GetRequiredService<DeploymentLoadPublisher>());
            
            this.logger.Debug("Creating {0} System Target", "RemoteGrainDirectory + CacheValidator");
            RegisterSystemTarget(this.LocalGrainDirectory.RemoteGrainDirectory);
            RegisterSystemTarget(this.LocalGrainDirectory.CacheValidator);

            this.logger.Debug("Creating {0} System Target", "RemoteClusterGrainDirectory");
            RegisterSystemTarget(this.LocalGrainDirectory.RemoteClusterGrainDirectory);

            this.logger.Debug("Creating {0} System Target", "ClientObserverRegistrar + TypeManager");

            this.RegisterSystemTarget(this.Services.GetRequiredService<ClientObserverRegistrar>());
            var implicitStreamSubscriberTable = this.Services.GetRequiredService<ImplicitStreamSubscriberTable>();
            var versionDirectorManager = this.Services.GetRequiredService<CachedVersionSelectorManager>();
            var grainTypeManager = this.Services.GetRequiredService<GrainTypeManager>();
            IOptions<TypeManagementOptions> typeManagementOptions = this.Services.GetRequiredService<IOptions<TypeManagementOptions>>();
            this.typeManager = new TypeManager(this.SiloAddress, this.SiloHostAddress, grainTypeManager, this.membershipOracle, this.LocalScheduler, typeManagementOptions.Value.TypeMapRefreshInterval, implicitStreamSubscriberTable, this.grainFactory, versionDirectorManager,
                this.loggerFactory);
            this.RegisterSystemTarget(this.typeManager);

            this.logger.Debug("Creating {0} System Target", "MembershipOracle");
            if (this.membershipOracle is SystemTarget)
            {
                RegisterSystemTarget((SystemTarget)this.membershipOracle);
            }

            if (this.multiClusterOracle != null && this.multiClusterOracle is SystemTarget)
            {
                this.logger.Debug("Creating {0} System Target", "MultiClusterOracle");
                RegisterSystemTarget((SystemTarget)this.multiClusterOracle);
            }

            var transactionAgent = this.Services.GetRequiredService<ITransactionAgent>() as SystemTarget;
            if (transactionAgent != null)
            {
                this.logger.Debug("Creating {0} System Target", "TransactionAgent");
                RegisterSystemTarget(transactionAgent);
            }

            this.logger.Debug("Finished creating System Targets for this silo.");
        }

        private async Task InjectDependencies()
        {
            this.healthCheckParticipants.Add(this.membershipOracle);

            this.catalog.SiloStatusOracle = this.membershipOracle;
            this.membershipOracle.SubscribeToSiloStatusEvents(this.localGrainDirectory);
            this.messageCenter.SiloDeadOracle = this.membershipOracle.IsDeadSilo;

            // consistentRingProvider is not a system target per say, but it behaves like the localGrainDirectory, so it is here
            this.membershipOracle.SubscribeToSiloStatusEvents((ISiloStatusListener)this.RingProvider);

            this.membershipOracle.SubscribeToSiloStatusEvents(this.typeManager);

            this.membershipOracle.SubscribeToSiloStatusEvents(this.Services.GetRequiredService<DeploymentLoadPublisher>());

            this.membershipOracle.SubscribeToSiloStatusEvents(this.Services.GetRequiredService<ClientObserverRegistrar>());

            IOptions<ReminderOptions> reminderOptions = this.Services.GetRequiredService<IOptions<ReminderOptions>>();
            if (!reminderOptions.Value.ReminderService.Equals(ReminderOptions.BuiltIn.Disabled))
            {
                // start the reminder service system target
                this.reminderService = this.Services.GetRequiredService<LocalReminderServiceFactory>()
                                          .CreateReminderService(this, this.initTimeout, this.runtimeClient);
                var reminderServiceSystemTarget = this.reminderService as SystemTarget;
                if (reminderServiceSystemTarget != null) RegisterSystemTarget(reminderServiceSystemTarget);
            }

            RegisterSystemTarget(this.catalog);
            await this.scheduler.QueueAction(this.catalog.Start, this.catalog.SchedulingContext)
                .WithTimeout(this.initTimeout);

            // SystemTarget for provider init calls
            this.fallbackScheduler = this.Services.GetRequiredService<FallbackSystemTarget>();

            RegisterSystemTarget(this.fallbackScheduler);
        }

        private Task OnRuntimeInitializeStart(CancellationToken ct)
        {
            lock (this.lockable)
            {
                if (!this.SystemStatus.Equals(SystemStatus.Created))
                    throw new InvalidOperationException(string.Format("Calling Silo.Start() on a silo which is not in the Created state. This silo is in the {0} state.", this.SystemStatus));

                this.SystemStatus = SystemStatus.Starting;
            }

            this.logger.Info(ErrorCode.SiloStarting, "Silo Start()");

            // Hook up to receive notification of process exit / Ctrl-C events
            AppDomain.CurrentDomain.ProcessExit += this.HandleProcessExit;
            if (this.siloOptions.FastKillOnCancelKeyPress)
                Console.CancelKeyPress += this.HandleProcessExit;
            //TODO: setup thead pool directly to lifecycle
            StartTaskWithPerfAnalysis("ConfigureThreadPoolAndServicePointSettings",
                this.ConfigureThreadPoolAndServicePointSettings, Stopwatch.StartNew());
            return Task.CompletedTask;
        }

        private void StartTaskWithPerfAnalysis(string taskName, Action task, Stopwatch stopWatch)
        {
            stopWatch.Restart();
            task.Invoke();
            stopWatch.Stop();
            this.logger.Info(ErrorCode.SiloStartPerfMeasure, $"{taskName} took {stopWatch.ElapsedMilliseconds} Milliseconds to finish");
        }

        private async Task StartAsyncTaskWithPerfAnalysis(string taskName, Func<Task> task, Stopwatch stopWatch)
        {
            stopWatch.Restart();
            await task.Invoke();
            stopWatch.Stop();
            this.logger.Info(ErrorCode.SiloStartPerfMeasure, $"{taskName} took {stopWatch.ElapsedMilliseconds} Milliseconds to finish");
        }

        private async Task OnRuntimeServicesStart(CancellationToken ct)
        {
            //TODO: Setup all (or as many as possible) of the class started in this call to work directly with lifecyce
            var stopWatch = Stopwatch.StartNew();
            // The order of these 4 is pretty much arbitrary.
            StartTaskWithPerfAnalysis("Start Scheduler", this.scheduler.Start, stopWatch);
            StartTaskWithPerfAnalysis("Start Message center", this.messageCenter.Start,stopWatch);
            StartTaskWithPerfAnalysis("Start Incoming message agents", IncomingMessageAgentsStart, stopWatch);
            void IncomingMessageAgentsStart()
            {
                this.incomingPingAgent.Start();
                this.incomingSystemAgent.Start();
                this.incomingAgent.Start();
            } 

            StartTaskWithPerfAnalysis("Start local grain directory", this.LocalGrainDirectory.Start,stopWatch);

            // Set up an execution context for this thread so that the target creation steps can use asynch values.
            RuntimeContext.InitializeMainThread();

            StartTaskWithPerfAnalysis("Init implicit stream subscribe table", InitImplicitStreamSubscribeTable, stopWatch);
            void InitImplicitStreamSubscribeTable()
            {             
                // Initialize the implicit stream subscribers table.
                var implicitStreamSubscriberTable = this.Services.GetRequiredService<ImplicitStreamSubscriberTable>();
                var grainTypeManager = this.Services.GetRequiredService<GrainTypeManager>();
                implicitStreamSubscriberTable.InitImplicitStreamSubscribers(grainTypeManager.GrainClassTypeData.Select(t => t.Value.Type).ToArray());
            }


            var siloProviderRuntime = this.Services.GetRequiredService<SiloProviderRuntime>();
            SiloStatisticsOptions statisticsOptions = this.Services.GetRequiredService<IOptions<SiloStatisticsOptions>>().Value;
            this.runtimeClient.CurrentStreamProviderRuntime = siloProviderRuntime;
            await StartAsyncTaskWithPerfAnalysis("Load StatisticProviders", LoadStatsProvider, stopWatch);
            async Task LoadStatsProvider()
            {
                // can call SetSiloMetricsTableDataManager only after MessageCenter is created (dependency on this.SiloAddress).
                await this.siloStatistics.SetSiloStatsTableDataManager(this, statisticsOptions).WithTimeout(this.initTimeout);
                await this.siloStatistics.SetSiloMetricsTableDataManager(this, statisticsOptions).WithTimeout(this.initTimeout);
            }
            
            // This has to follow the above steps that start the runtime components
            await StartAsyncTaskWithPerfAnalysis("Create system targets and inject dependencies", () =>
            {
                CreateSystemTargets();
                return InjectDependencies();
            }, stopWatch);

            // Validate the configuration.
            // TODO - refactor validation - jbragg
            //GlobalConfig.Application.ValidateConfiguration(logger);
        }

        private async Task OnRuntimeGrainServicesStart(CancellationToken ct)
        {
            var stopWatch = Stopwatch.StartNew();

            await StartAsyncTaskWithPerfAnalysis("Init transaction agent", InitTransactionAgent, stopWatch);
            async Task InitTransactionAgent()
            {
                ITransactionAgent transactionAgent = this.Services.GetRequiredService<ITransactionAgent>();
                ISchedulingContext transactionAgentContext = (transactionAgent as SystemTarget)?.SchedulingContext;
                await scheduler.QueueTask(transactionAgent.Start, transactionAgentContext)
                    .WithTimeout(initTimeout);
            }

            // Load and init grain services before silo becomes active.
            GrainServiceOptions grainServiceOptions = Services.GetRequiredService<IOptions<GrainServiceOptions>>().Value;
            await StartAsyncTaskWithPerfAnalysis("Init grain services",
                () => CreateGrainServices(grainServiceOptions), stopWatch);

            this.membershipOracleContext = (this.membershipOracle as SystemTarget)?.SchedulingContext ??
                                       this.fallbackScheduler.SchedulingContext;

            await StartAsyncTaskWithPerfAnalysis("Start local silo status oracle", StartMembershipOracle, stopWatch);

            async Task StartMembershipOracle()
            {
                await this.scheduler.QueueTask(() => this.membershipOracle.Start(), this.membershipOracleContext)
                    .WithTimeout(this.initTimeout);
                this.logger.Debug("Local silo status oracle created successfully.");
                await this.scheduler.QueueTask(this.membershipOracle.BecomeActive, this.membershipOracleContext)
                    .WithTimeout(this.initTimeout);
                this.logger.Debug("Local silo status oracle became active successfully.");
            }

            var versionStore = this.Services.GetService<IVersionStore>();
            await StartAsyncTaskWithPerfAnalysis("Init type manager", () => this.scheduler
                .QueueTask(() => this.typeManager.Initialize(versionStore), this.typeManager.SchedulingContext)
                .WithTimeout(this.initTimeout), stopWatch);

            //if running in multi cluster scenario, start the MultiClusterNetwork Oracle
            if (this.multiClusterOracle != null)
            {
                await StartAsyncTaskWithPerfAnalysis("Start multicluster oracle", StartMultiClusterOracle, stopWatch);
                async Task StartMultiClusterOracle()
                {
                    this.logger.Info("Starting multicluster oracle with my ServiceId={0} and ClusterId={1}.",
                        this.siloOptions.ServiceId, this.siloOptions.ClusterId);

                    this.multiClusterOracleContext = (this.multiClusterOracle as SystemTarget)?.SchedulingContext ??
                                                     this.fallbackScheduler.SchedulingContext;
                    await this.scheduler.QueueTask(() => this.multiClusterOracle.Start(), this.multiClusterOracleContext)
                        .WithTimeout(this.initTimeout);
                    this.logger.Debug("multicluster oracle created successfully.");
                }
            }

            try
            {
                SiloStatisticsOptions statisticsOptions = this.Services.GetRequiredService<IOptions<SiloStatisticsOptions>>().Value;
                StartTaskWithPerfAnalysis("Start silo statistics", () => this.siloStatistics.Start(statisticsOptions), stopWatch);
                this.logger.Debug("Silo statistics manager started successfully.");

                // Finally, initialize the deployment load collector, for grains with load-based placement
                await StartAsyncTaskWithPerfAnalysis("Start deployment load collector", StartDeploymentLoadCollector, stopWatch);
                async Task StartDeploymentLoadCollector()
                {
                    var deploymentLoadPublisher = this.Services.GetRequiredService<DeploymentLoadPublisher>();
                    await this.scheduler.QueueTask(deploymentLoadPublisher.Start, deploymentLoadPublisher.SchedulingContext)
                        .WithTimeout(this.initTimeout);
                    this.logger.Debug("Silo deployment load publisher started successfully.");
                }


                // Start background timer tick to watch for platform execution stalls, such as when GC kicks in
                this.platformWatchdog = new Watchdog(statisticsOptions.LogWriteInterval, this.healthCheckParticipants, this.executorService, this.loggerFactory);
                this.platformWatchdog.Start();
                if (this.logger.IsEnabled(LogLevel.Debug)) { this.logger.Debug("Silo platform watchdog started successfully."); }

                if (this.reminderService != null)
                {
                    await StartAsyncTaskWithPerfAnalysis("Start reminder service", StartReminderService, stopWatch);
                    async Task StartReminderService()
                    {
                        // so, we have the view of the membership in the consistentRingProvider. We can start the reminder service
                        this.reminderServiceContext = (this.reminderService as SystemTarget)?.SchedulingContext ??
                                                      this.fallbackScheduler.SchedulingContext;
                        await this.scheduler.QueueTask(this.reminderService.Start, this.reminderServiceContext)
                            .WithTimeout(this.initTimeout);
                        this.logger.Debug("Reminder service started successfully.");
                    }
                }

                StartTaskWithPerfAnalysis("Start gateway", StartGateway, stopWatch);
                void StartGateway()
                {
                    // Now that we're active, we can start the gateway
                    var mc = this.messageCenter as MessageCenter;
                    mc?.StartGateway(this.Services.GetRequiredService<ClientObserverRegistrar>());
                    this.logger.Debug("Message gateway service started successfully.");
                }
                this.SystemStatus = SystemStatus.Running;
            }
            catch (Exception exc)
            {
                this.SafeExecute(() => this.logger.Error(ErrorCode.Runtime_Error_100330, string.Format("Error starting silo {0}. Going to FastKill().", this.SiloAddress), exc));
                throw;
            }
            if (this.logger.IsEnabled(LogLevel.Debug)) { this.logger.Debug("Silo.Start complete: System status = {0}", this.SystemStatus); }
        }

        private async Task CreateGrainServices(GrainServiceOptions grainServiceOptions)
        {
            foreach (KeyValuePair<string, short> serviceConfig in grainServiceOptions.GrainServices)
            {
                // Construct the Grain Service
                var serviceType = System.Type.GetType(serviceConfig.Key);
                if (serviceType == null)
                {
                    throw new Exception(string.Format("Cannot find Grain Service type {0} of with Service Id {1}", serviceConfig.Key, serviceConfig.Value));
                }
                
                var grainServiceInterfaceType = serviceType.GetInterfaces().FirstOrDefault(x => x.GetInterfaces().Contains(typeof(IGrainService)));
                if (grainServiceInterfaceType == null)
                {
                    throw new Exception(string.Format("Cannot find an interface on {0} which implements IGrainService", serviceConfig.Value));
                }

                var typeCode = GrainInterfaceUtils.GetGrainClassTypeCode(grainServiceInterfaceType);
                var grainId = (IGrainIdentity)GrainId.GetGrainServiceGrainId(serviceConfig.Value, typeCode);
                var grainService = (GrainService)ActivatorUtilities.CreateInstance(this.Services, serviceType, grainId);
                RegisterSystemTarget(grainService);

                await this.scheduler.QueueTask(() => grainService.Init(this.Services), grainService.SchedulingContext).WithTimeout(this.initTimeout);
                await this.scheduler.QueueTask(grainService.Start, grainService.SchedulingContext).WithTimeout(this.initTimeout);
                if (this.logger.IsEnabled(LogLevel.Debug))
                {
                    this.logger.Debug(string.Format("{0} Grain Service with Id {1} started successfully.", serviceConfig.Key, serviceConfig.Value));
                }
            }
        }

        private void ConfigureThreadPoolAndServicePointSettings()
        {
            ThreadPoolOptions threadPoolOptions = this.Services.GetRequiredService<IOptions<ThreadPoolOptions>>().Value;
            if (threadPoolOptions.MinDotNetThreadPoolSize > 0)
            {
                int workerThreads;
                int completionPortThreads;
                ThreadPool.GetMinThreads(out workerThreads, out completionPortThreads);
                if (threadPoolOptions.MinDotNetThreadPoolSize > workerThreads ||
                    threadPoolOptions.MinDotNetThreadPoolSize > completionPortThreads)
                {
                    // if at least one of the new values is larger, set the new min values to be the larger of the prev. and new config value.
                    int newWorkerThreads = Math.Max(threadPoolOptions.MinDotNetThreadPoolSize, workerThreads);
                    int newCompletionPortThreads = Math.Max(threadPoolOptions.MinDotNetThreadPoolSize, completionPortThreads);
                    bool ok = ThreadPool.SetMinThreads(newWorkerThreads, newCompletionPortThreads);
                    if (ok)
                    {
                        this.logger.Info(ErrorCode.SiloConfiguredThreadPool,
                                    "Configured ThreadPool.SetMinThreads() to values: {0},{1}. Previous values are: {2},{3}.",
                                    newWorkerThreads, newCompletionPortThreads, workerThreads, completionPortThreads);
                    }
                    else
                    {
                        this.logger.Warn(ErrorCode.SiloFailedToConfigureThreadPool,
                                    "Failed to configure ThreadPool.SetMinThreads(). Tried to set values to: {0},{1}. Previous values are: {2},{3}.",
                                    newWorkerThreads, newCompletionPortThreads, workerThreads, completionPortThreads);
                    }
                }
            }

            // Set .NET ServicePointManager settings to optimize throughput performance when using Azure storage
            // http://blogs.msdn.com/b/windowsazurestorage/archive/2010/06/25/nagle-s-algorithm-is-not-friendly-towards-small-requests.aspx
            ServicePointOptions servicePointOptions = this.Services.GetRequiredService<IOptions<ServicePointOptions>>().Value;
            this.logger.Info(ErrorCode.SiloConfiguredServicePointManager,
                "Configured .NET ServicePointManager to Expect100Continue={0}, DefaultConnectionLimit={1}, UseNagleAlgorithm={2} to improve Azure storage performance.",
                servicePointOptions.Expect100Continue, servicePointOptions.DefaultConnectionLimit, servicePointOptions.UseNagleAlgorithm);
            ServicePointManager.Expect100Continue = servicePointOptions.Expect100Continue;
            ServicePointManager.DefaultConnectionLimit = servicePointOptions.DefaultConnectionLimit;
            ServicePointManager.UseNagleAlgorithm = servicePointOptions.UseNagleAlgorithm;
        }

        /// <summary>
        /// Gracefully stop the run time system only, but not the application. 
        /// Applications requests would be abruptly terminated, while the internal system state gracefully stopped and saved as much as possible.
        /// Grains are not deactivated.
        /// </summary>
        public void Stop()
        {
            var cancellationSource = new CancellationTokenSource();
            cancellationSource.Cancel();
            StopAsync(cancellationSource.Token).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Gracefully stop the run time system and the application. 
        /// All grains will be properly deactivated.
        /// All in-flight applications requests would be awaited and finished gracefully.
        /// </summary>
        public void Shutdown()
        {
            StopAsync(CancellationToken.None).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Gracefully stop the run time system only, but not the application. 
        /// Applications requests would be abruptly terminated, while the internal system state gracefully stopped and saved as much as possible.
        /// </summary>
        public Task StopAsync(CancellationToken cancellationToken)
        {
            bool gracefully = !cancellationToken.IsCancellationRequested;
            string operation = gracefully ? "Shutdown()" : "Stop()";
            bool stopAlreadyInProgress = false;
            lock (this.lockable)
            {
                if (this.SystemStatus.Equals(SystemStatus.Stopping) ||
                    this.SystemStatus.Equals(SystemStatus.ShuttingDown) ||
                    this.SystemStatus.Equals(SystemStatus.Terminated))
                {
                    stopAlreadyInProgress = true;
                    // Drop through to wait below
                }
                else if (!this.SystemStatus.Equals(SystemStatus.Running))
                {
                    throw new InvalidOperationException(string.Format("Calling Silo.{0} on a silo which is not in the Running state. This silo is in the {1} state.", operation, this.SystemStatus));
                }
                else
                {
                    if (gracefully)
                        this.SystemStatus = SystemStatus.ShuttingDown;
                    else
                        this.SystemStatus = SystemStatus.Stopping;
                }
            }

            if (stopAlreadyInProgress)
            {
                this.logger.Info(ErrorCode.SiloStopInProgress, "Silo termination is in progress - Will wait for it to finish");
                var pause = TimeSpan.FromSeconds(1);
                while (!this.SystemStatus.Equals(SystemStatus.Terminated))
                {
                    this.logger.Info(ErrorCode.WaitingForSiloStop, "Waiting {0} for termination to complete", pause);
                    Thread.Sleep(pause);
                }
                return this.siloTerminatedTask.Task;
            }
            return this.siloLifecycle.OnStop(cancellationToken);
        }

        private Task OnRuntimeServicesStop(CancellationToken cancellationToken)
        {
            if (!cancellationToken.IsCancellationRequested)
            {
                // Deactivate all grains
                SafeExecute(() => this.catalog.DeactivateAllActivations().WaitWithThrow(this.stopTimeout));
            }

            // Stop the gateway
            SafeExecute(this.messageCenter.StopAcceptingClientMessages);

            // Start rejecting all silo to silo application messages
            SafeExecute(this.messageCenter.BlockApplicationMessages);

            // Stop scheduling/executing application turns
            SafeExecute(this.scheduler.StopApplicationTurns);

            // Directory: Speed up directory handoff
            // will be started automatically when directory receives SiloStatusChangeNotification(Stopping)

            SafeExecute(() => this.LocalGrainDirectory.StopPreparationCompletion.WaitWithThrow(this.stopTimeout));

            return Task.CompletedTask;
        }

        private async Task OnRuntimeGrainServicesStop(CancellationToken cancellationToken)
        {
            bool gracefully = !cancellationToken.IsCancellationRequested;
            string operation = gracefully ? "Shutdown()" : "Stop()";
            try
            {
                if (gracefully)
                {
                    this.logger.Info(ErrorCode.SiloShuttingDown, "Silo starting to Shutdown()");
                    // 1: Write "ShutDown" state in the table + broadcast gossip msgs to re-read the table to everyone
                    await this.scheduler.QueueTask(this.membershipOracle.ShutDown, this.membershipOracleContext)
                        .WithTimeout(this.stopTimeout);
                }
                else
                {
                    this.logger.Info(ErrorCode.SiloStopping, "Silo starting to Stop()");
                    // 1: Write "Stopping" state in the table + broadcast gossip msgs to re-read the table to everyone
                    await this.scheduler.QueueTask(this.membershipOracle.Stop, this.membershipOracleContext)
                        .WithTimeout(this.stopTimeout);
                }
            }
            catch (Exception exc)
            {
                this.logger.Error(ErrorCode.SiloFailedToStopMembership, string.Format("Failed to {0} membership oracle. About to FastKill this silo.", operation), exc);
                return; // will go to finally
            }

            if (this.reminderService != null)
            {
                // 2: Stop reminder service
                await this.scheduler.QueueTask(this.reminderService.Stop, this.reminderServiceContext)
                    .WithTimeout(this.stopTimeout);
            }
        }

        private Task OnRuntimeInitializeStop(CancellationToken ct)
        {
            // 10, 11, 12: Write Dead in the table, Drain scheduler, Stop msg center, ...
            this.logger.Info(ErrorCode.SiloStopped, "Silo is Stopped()");

            SafeExecute(() => this.scheduler.QueueTask( this.membershipOracle.KillMyself, this.membershipOracleContext)
                .WaitWithThrow(this.stopTimeout));

            // incoming messages
            SafeExecute(this.incomingSystemAgent.Stop);
            SafeExecute(this.incomingPingAgent.Stop);
            SafeExecute(this.incomingAgent.Stop);

            // timers
            if (this.platformWatchdog != null) 
                SafeExecute(this.platformWatchdog.Stop); // Silo may be dying before platformWatchdog was set up

            SafeExecute(this.scheduler.Stop);
            SafeExecute(this.scheduler.PrintStatistics);
            SafeExecute(this.activationDirectory.PrintActivationDirectory);
            SafeExecute(this.messageCenter.Stop);
            SafeExecute(this.siloStatistics.Stop);

            SafeExecute(() => this.SystemStatus = SystemStatus.Terminated);
            SafeExecute(() => (this.Services as IDisposable)?.Dispose());

            // Setting the event should be the last thing we do.
            // Do nothing after that!
            this.siloTerminatedTask.SetResult(0);
            return Task.CompletedTask;
        }

        private void SafeExecute(Action action)
        {
            Utils.SafeExecute(action, this.logger, "Silo.Stop");
        }

        private void HandleProcessExit(object sender, EventArgs e)
        {
            // NOTE: We need to minimize the amount of processing occurring on this code path -- we only have under approx 2-3 seconds before process exit will occur
            this.logger.Warn(ErrorCode.Runtime_Error_100220, "Process is exiting");
            
            lock (this.lockable)
            {
                if (!this.SystemStatus.Equals(SystemStatus.Running)) return;
                    
                //this.SystemStatus = SystemStatus.Stopping;
            }
                
            this.logger.Info(ErrorCode.SiloStopping, "Silo.HandleProcessExit() - starting to FastKill()");
            Stop();
        }

        internal void RegisterSystemTarget(SystemTarget target)
        {
            var providerRuntime = this.Services.GetRequiredService<SiloProviderRuntime>();
            providerRuntime.RegisterSystemTarget(target);
        }

        /// <summary> Return dump of diagnostic data from this silo. </summary>
        /// <param name="all"></param>
        /// <returns>Debug data for this silo.</returns>
        public string GetDebugDump(bool all = true)
        {
            var sb = new StringBuilder();            
            foreach (var systemTarget in this.activationDirectory.AllSystemTargets())
                sb.AppendFormat("System target {0}:", ((ISystemTargetBase)systemTarget).GrainId.ToString()).AppendLine();               
            
            var enumerator = this.activationDirectory.GetEnumerator();
            while(enumerator.MoveNext())
            {
                Utils.SafeExecute(() =>
                {
                    var activationData = enumerator.Current.Value;
                    var workItemGroup = this.scheduler.GetWorkItemGroup(activationData.SchedulingContext);
                    if (workItemGroup == null)
                    {
                        sb.AppendFormat("Activation with no work item group!! Grain {0}, activation {1}.",
                            activationData.Grain,
                            activationData.ActivationId);
                        sb.AppendLine();
                        return;
                    }

                    if (all || activationData.State.Equals(ActivationState.Valid))
                    {
                        sb.AppendLine(workItemGroup.DumpStatus());
                        sb.AppendLine(activationData.DumpStatus());
                    }
                });
            }
            this.logger.Info(ErrorCode.SiloDebugDump, sb.ToString());
            return sb.ToString();
        }

        /// <summary> Object.ToString override -- summary info for this silo. </summary>
        public override string ToString()
        {
            return this.localGrainDirectory.ToString();
        }

        private void Participate(ISiloLifecycle lifecycle)
        {
            lifecycle.Subscribe(SiloLifecycleStage.RuntimeInitialize, this.OnRuntimeInitializeStart, this.OnRuntimeInitializeStop);
            lifecycle.Subscribe(SiloLifecycleStage.RuntimeServices, this.OnRuntimeServicesStart, this.OnRuntimeServicesStop);
            lifecycle.Subscribe(SiloLifecycleStage.RuntimeGrainServices, this.OnRuntimeGrainServicesStart, this.OnRuntimeGrainServicesStop);
            
        }
    }

    // A dummy system target for fallback scheduler
    internal class FallbackSystemTarget : SystemTarget
    {
        public FallbackSystemTarget(ILocalSiloDetails localSiloDetails, ILoggerFactory loggerFactory)
            : base(Constants.FallbackSystemTargetId, localSiloDetails.SiloAddress, localSiloDetails.HostSiloAddress, loggerFactory)
        {
        }
    }
}

