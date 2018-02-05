using System;
using System.Net;
using System.Threading;
using Microsoft.Extensions.Logging;
using Orleans.Messaging;
using Orleans.Runtime.Configuration;
using Orleans.Serialization;
using Microsoft.Extensions.Options;
using Orleans.Hosting;

namespace Orleans.Runtime.Messaging
{
    internal class MessageCenter : ISiloMessageCenter, IDisposable
    {
        private Gateway Gateway { get; set; }
        private IncomingMessageAcceptor ima;
        private readonly ILogger log;
        private Action<Message> rerouteHandler;
        internal Func<Message, bool> ShouldDrop;

        // ReSharper disable NotAccessedField.Local
        private IntValueStatistic sendQueueLengthCounter;
        private IntValueStatistic receiveQueueLengthCounter;
        // ReSharper restore NotAccessedField.Local

        internal IOutboundMessageQueue OutboundQueue { get; set; }
        internal IInboundMessageQueue InboundQueue { get; set; }
        internal SocketManager SocketManager;
        private readonly SerializationManager serializationManager;
        private readonly MessageFactory messageFactory;
        private readonly ILoggerFactory loggerFactory;
        private readonly ExecutorService executorService;

        internal bool IsBlockingApplicationMessages { get; private set; }
        internal ISiloPerformanceMetrics Metrics { get; private set; }
        
        public bool IsProxying { get { return this.Gateway != null; } }

        public bool TryDeliverToProxy(Message msg)
        {
            return msg.TargetGrain.IsClient && this.Gateway != null && this.Gateway.TryDeliverToProxy(msg);
        }
        
        // This is determined by the IMA but needed by the OMS, and so is kept here in the message center itself.
        public SiloAddress MyAddress { get; private set; }
        public SiloAddress MyHostAddress { get; private set; }

        public MessageCenter(
            ILocalSiloDetails siloDetails,
            IOptions<SiloMessagingOptions> messagingOptions,
            IOptions<NetworkingOptions> networkingOptions,
            SerializationManager serializationManager,
            ISiloPerformanceMetrics metrics,
            MessageFactory messageFactory,
            Factory<MessageCenter, Gateway> gatewayFactory,
            ExecutorService executorService,
            ILoggerFactory loggerFactory)
        {
            this.loggerFactory = loggerFactory;
            this.log = loggerFactory.CreateLogger<MessageCenter>();
            this.serializationManager = serializationManager;
            this.messageFactory = messageFactory;
            this.executorService = executorService;
            this.Initialize(siloDetails.SiloAddress.Endpoint, siloDetails.HostSiloAddress?.Endpoint, siloDetails.SiloAddress.Generation, messagingOptions, networkingOptions, metrics);
            if (siloDetails.GatewayAddress != null)
            {
                this.Gateway = gatewayFactory(this);
            }
        }

        private void Initialize(IPEndPoint here, IPEndPoint hostEndpoint, int generation, IOptions<SiloMessagingOptions> messagingOptions, IOptions<NetworkingOptions> networkingOptions, ISiloPerformanceMetrics metrics = null)
        {
            if(this.log.IsEnabled(LogLevel.Trace)) this.log.Trace("Starting initialization.");

            this.SocketManager = new SocketManager(networkingOptions, this.loggerFactory);
            this.ima = new IncomingMessageAcceptor(this, here, SocketDirection.SiloToSilo, this.messageFactory, this.serializationManager, this.executorService, this.loggerFactory);
            this.MyAddress = SiloAddress.New((IPEndPoint)this.ima.AcceptingSocket.LocalEndPoint, generation);
            if (hostEndpoint != null)
            {
                this.MyHostAddress = SiloAddress.New(hostEndpoint, generation);
            }

            this.InboundQueue = new InboundMessageQueue(this.loggerFactory);
            this.OutboundQueue = new OutboundMessageQueue(this, messagingOptions, this.serializationManager, this.executorService, this.loggerFactory);
            this.Metrics = metrics;

            this.sendQueueLengthCounter = IntValueStatistic.FindOrCreate(StatisticNames.MESSAGE_CENTER_SEND_QUEUE_LENGTH, () => this.SendQueueLength);
            this.receiveQueueLengthCounter = IntValueStatistic.FindOrCreate(StatisticNames.MESSAGE_CENTER_RECEIVE_QUEUE_LENGTH, () => this.ReceiveQueueLength);

            if (this.log.IsEnabled(LogLevel.Trace)) this.log.Trace("Completed initialization.");
        }

        public void Start()
        {
            this.IsBlockingApplicationMessages = false;
            this.ima.Start();
            this.OutboundQueue.Start();
        }

        public void StartGateway(ClientObserverRegistrar clientRegistrar)
        {
            if (this.Gateway != null)
                this.Gateway.Start(clientRegistrar);
        }

        public void PrepareToStop()
        {
        }

        public void Stop()
        {
            this.IsBlockingApplicationMessages = true;

            try
            {
                this.ima.Stop();
            }
            catch (Exception exc)
            {
                this.log.Error(ErrorCode.Runtime_Error_100108, "Stop failed.", exc);
            }

            StopAcceptingClientMessages();

            try
            {
                this.OutboundQueue.Stop();
            }
            catch (Exception exc)
            {
                this.log.Error(ErrorCode.Runtime_Error_100110, "Stop failed.", exc);
            }

            try
            {
                this.SocketManager.Stop();
            }
            catch (Exception exc)
            {
                this.log.Error(ErrorCode.Runtime_Error_100111, "Stop failed.", exc);
            }
        }

        public void StopAcceptingClientMessages()
        {
            if (this.log.IsEnabled(LogLevel.Debug)) this.log.Debug("StopClientMessages");
            if (this.Gateway == null) return;

            try
            {
                this.Gateway.Stop();
            }
            catch (Exception exc) { this.log.Error(ErrorCode.Runtime_Error_100109, "Stop failed.", exc); }
            this.Gateway = null;
        }

        public Action<Message> RerouteHandler
        {
            set
            {
                if (this.rerouteHandler != null)
                    throw new InvalidOperationException("MessageCenter RerouteHandler already set");
                this.rerouteHandler = value;
            }
        }

        public void RerouteMessage(Message message)
        {
            if (this.rerouteHandler != null)
                this.rerouteHandler(message);
            else
                SendMessage(message);
        }

        public Action<Message> SniffIncomingMessage
        {
            set
            {
                this.ima.SniffIncomingMessage = value;
            }
        }

        public Func<SiloAddress, bool> SiloDeadOracle { get; set; }

        public void SendMessage(Message msg)
        {
            // Note that if we identify or add other grains that are required for proper stopping, we will need to treat them as we do the membership table grain here.
            if (this.IsBlockingApplicationMessages && (msg.Category == Message.Categories.Application) && (msg.Result != Message.ResponseTypes.Rejection)
                && !Constants.SystemMembershipTableId.Equals(msg.TargetGrain))
            {
                // Drop the message on the floor if it's an application message that isn't a rejection
            }
            else
            {
                if (msg.SendingSilo == null)
                    msg.SendingSilo = this.MyHostAddress ?? this.MyAddress;
                this.OutboundQueue.SendMessage(msg);
            }
        }

        internal void SendRejection(Message msg, Message.RejectionTypes rejectionType, string reason)
        {
            MessagingStatisticsGroup.OnRejectedMessage(msg);
            if (string.IsNullOrEmpty(reason)) reason = string.Format("Rejection from silo {0}{1} - Unknown reason.",
                this.MyAddress, this.MyHostAddress != null ? $"(hosted in {this.MyHostAddress})" : string.Empty);
            Message error = this.messageFactory.CreateRejectionResponse(msg, rejectionType, reason);
            // rejection msgs are always originated in the local silo, they are never remote.
            this.InboundQueue.PostMessage(error);
        }

        public Message WaitMessage(Message.Categories type, CancellationToken ct)
        {
            return this.InboundQueue.WaitMessage(type);
        }

        public void Dispose()
        {
            if (this.ima != null)
            {
                this.ima.Dispose();
                this.ima = null;
            }

            this.InboundQueue?.Dispose();
            this.OutboundQueue?.Dispose();

            GC.SuppressFinalize(this);
        }

        public int SendQueueLength { get { return this.OutboundQueue.Count; } }

        public int ReceiveQueueLength { get { return this.InboundQueue.Count; } }

        /// <summary>
        /// Indicates that application messages should be blocked from being sent or received.
        /// This method is used by the "fast stop" process.
        /// <para>
        /// Specifically, all outbound application messages are dropped, except for rejections and messages to the membership table grain.
        /// Inbound application requests are rejected, and other inbound application messages are dropped.
        /// </para>
        /// </summary>
        public void BlockApplicationMessages()
        {
            if(this.log.IsEnabled(LogLevel.Debug)) this.log.Debug("BlockApplicationMessages");
            this.IsBlockingApplicationMessages = true;
        }
    }
}
