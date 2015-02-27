using System;
using System.Data.SqlClient;
using System.Transactions;
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;

namespace ConsoleApplication4
{
    class Program
    {
        private static string sqlConnectionString = "";
        private static string asbConnectionString = "";

        static void Main(string[] args)
        {

                //var namespaceManager = NamespaceManager.CreateFromConnectionString(asbConnectionString);
                //namespaceManager.CreateQueue("spikes/worker");
                //namespaceManager.CreateQueue("spikes/audit");
                //namespaceManager.CreateQueue("spikes/worker.xfer");
                //namespaceManager.CreateTopic("spikes/worker.events");
                //namespaceManager.CreateQueue("spikes/results");
                //    namespaceManager.CreateSubscription(
                //        new SubscriptionDescription("spikes/worker.events", "spikes.results")
                //        {
                //            ForwardTo = "spikes/results",
                //        });

                var connectionStringBuilder =
                new ServiceBusConnectionStringBuilder(asbConnectionString);

                var factory1 =
                    MessagingFactory.Create(
                            connectionStringBuilder.Endpoints,
                            new MessagingFactorySettings()
                            {
                                TokenProvider = TokenProvider.CreateSharedAccessSignatureTokenProvider(connectionStringBuilder.SharedAccessKeyName, connectionStringBuilder.SharedAccessKey),
                                TransportType = TransportType.NetMessaging,
                                NetMessagingTransportSettings = new NetMessagingTransportSettings()
                            });

                var factory2 =
                    MessagingFactory.Create(
                           connectionStringBuilder.Endpoints,
                           new MessagingFactorySettings()
                           {
                               TokenProvider = TokenProvider.CreateSharedAccessSignatureTokenProvider(connectionStringBuilder.SharedAccessKeyName, connectionStringBuilder.SharedAccessKey),
                               TransportType = TransportType.NetMessaging,
                               NetMessagingTransportSettings = new NetMessagingTransportSettings()
                           });

                var factory3 =
                   MessagingFactory.Create(
                           connectionStringBuilder.Endpoints,
                           new MessagingFactorySettings()
                           {
                               TokenProvider = TokenProvider.CreateSharedAccessSignatureTokenProvider(connectionStringBuilder.SharedAccessKeyName, connectionStringBuilder.SharedAccessKey),
                               TransportType = TransportType.NetMessaging,
                               NetMessagingTransportSettings = new NetMessagingTransportSettings()
                           });

                // Dump two messages in the receive queue to use for testing
                var sender = factory1.CreateMessageSender("spikes/worker");

                Console.WriteLine("Enter [s], [ss], [sse], [svu], [svs] or [x] to stop...");
                var x = Console.ReadLine();
                if (x != "x")
                {
                    sender.SendBatch(new[] { new BrokeredMessage() { Label = "Message 1", TimeToLive = TimeSpan.FromSeconds(60) }, new BrokeredMessage() { Label = "Message 2", TimeToLive = TimeSpan.FromSeconds(60) }, });
                }

                while (x != "x")
                {
                    
                    
                    BrokeredMessage message = null;

                    // Start a new TX
                    using (var scope = new TransactionScope(TransactionScopeOption.RequiresNew))
                    {
                        // Receive the message
                        var receiver = factory1.CreateMessageReceiver("spikes/worker", ReceiveMode.PeekLock);
                        Console.WriteLine("Receiving message, this may take up to 5 seconds");
                        message = receiver.Receive(TimeSpan.FromSeconds(5));

                        if (message != null)
                        {
                            Console.WriteLine("Received message");

                            try
                            {
                                 // Create the publisher, it uses the receive queue as a transfer queue
                                var xferPublisher = factory2.CreateMessageSender("spikes/worker.events", "spikes/worker");

                                // Create the audit sender, it uses the receive queue as a transfer queue
                                var xferAudit = factory3.CreateMessageSender("spikes/audit", "spikes/worker");

                                if (x == "s")
                                {
                                    Console.WriteLine("This will throw a whole lot of exceptions until the messages are on the deadletter queue");

                                    SendSync(xferPublisher, new BrokeredMessage() {Label = "Published: " + message.Label});
                                    SendSync(xferAudit, new BrokeredMessage() { Label = "Processed: " + message.Label });
                                }
                                if (x == "ss")
                                {
                                    Console.WriteLine("No exceptions expected, messages should be sent to audit & result");

                                    SendSyncSuppressed(xferPublisher, new BrokeredMessage() { Label = "Published: " + message.Label });
                                    SendSyncSuppressed(xferAudit, new BrokeredMessage() { Label = "Processed: " + message.Label });
                                }
                                if (x == "sse")
                                {
                                    Console.WriteLine("Exceptions expected, receive message will roll back and eventually end up on deadletter queue, but messages will also be sent to result (not to audit as it already blew up)");

                                    SendSyncSuppressedWithExceptionInUserCode(xferPublisher, new BrokeredMessage() { Label = "Published: " + message.Label });
                                    SendSyncSuppressedWithExceptionInUserCode(xferAudit, new BrokeredMessage() { Label = "Processed: " + message.Label });
                                }
                                if (x == "svu")
                                {
                                    Console.WriteLine("Exceptions expected in user code, receive message will roll back and eventually end up on deadletter queue, no messages will be sent to audit & result");

                                    SendSyncVolatileWithExceptionInUserCode(xferPublisher, new BrokeredMessage() { Label = "Published: " + message.Label });
                                    SendSyncVolatileWithExceptionInUserCode(xferAudit, new BrokeredMessage() { Label = "Processed: " + message.Label });
                                }
                                if (x == "svs")
                                {
                                    Console.WriteLine("Exceptions expected on send, receive message will not roll back, no messages will  be sent to audit & result");
                                    Console.WriteLine("Note, this is considered message loss, but mitigations can be put into place like retries, failover namespaces or temporal durable backoff");

                                    SendSyncVolatileWithExceptionOnSend(xferPublisher, new BrokeredMessage() { Label = "Published: " + message.Label });
                                    SendSyncVolatileWithExceptionOnSend(xferAudit, new BrokeredMessage() { Label = "Processed: " + message.Label });
                                }

                                // if not suppressed will blow up stating that transaction not supported
                                using (var suppressScope = new TransactionScope(TransactionScopeOption.Suppress))
                                {
                                    message.Complete();
                                    suppressScope.Complete();
                                }

                                scope.Complete();
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine(ex);

                                try
                                {
                                    // if not suppressed will blow up stating that transaction not supported
                                    using (var suppressScope = new TransactionScope(TransactionScopeOption.Suppress))
                                    {
                                        message.Abandon();
                                        suppressScope.Complete();
                                    }
                                }
                                catch (TransactionException)
                                {
                                    Console.WriteLine("Looks like we can't abandon in this model");
                                }
                                catch (Exception e)
                                {
                                    Console.WriteLine("Unexpected exception: " + e);
                                }
                            }
                        }
                        else
                        {
                            scope.Complete();
                        }
                    }

                    if (message == null)
                    {
                        Console.WriteLine("Check your queues for the results and clean results & audit if needed");
                        Console.WriteLine("Enter [s], [ss], [sse], [svu], [svs] or [x] to stop...");
                        x = Console.ReadLine();
                        if (x != "x")
                        {
                            sender.SendBatch(new[] { new BrokeredMessage() { Label = "Message 1", TimeToLive = TimeSpan.FromSeconds(60) }, new BrokeredMessage() { Label = "Message 2", TimeToLive = TimeSpan.FromSeconds(60) }, });
                        }
                    }
                  
                }
                


            Console.WriteLine("Done...");
            Console.ReadLine();
        }

        private static void SendSync(MessageSender xferPublisher, BrokeredMessage message)
        {
            Console.WriteLine("Sending message without nested scope,");

            using (var scope = new TransactionScope(TransactionScopeOption.Required))
            {
                using (var conn = new SqlConnection(sqlConnectionString))
                {
                    conn.Open();

                    xferPublisher.Send(message); // will blow up stating that transaction not supported

                    scope.Complete();
                }
            }
        }

        private static void SendSyncSuppressed(MessageSender xferPublisher, BrokeredMessage message)
        {
            Console.WriteLine("Sending message with nested scope, no exception expected");

            using (var scope = new TransactionScope(TransactionScopeOption.Required))
            {
                using (var conn = new SqlConnection(sqlConnectionString))
                {
                    conn.Open();

                    using (var scope2 = new TransactionScope(TransactionScopeOption.Suppress))
                    {
                        xferPublisher.Send(message);

                        scope2.Complete();
                    }

                    scope.Complete();
                }
            }
        }

        private static void SendSyncSuppressedWithExceptionInUserCode(MessageSender xferPublisher, BrokeredMessage message)
        {
            Console.WriteLine("Sending message with nested scope with exception in user code");

            using (var scope = new TransactionScope(TransactionScopeOption.Required))
            {
                using (var conn = new SqlConnection(sqlConnectionString))
                {
                    conn.Open();
                    using (var scope2 = new TransactionScope(TransactionScopeOption.Suppress))
                    {
                        xferPublisher.Send(message);

                        scope2.Complete();
                    }

                    throw new Exception("Will this roll back?"); // yes, but still sending

                    scope.Complete();
                }
                
            }
        }

        private static void SendSyncVolatileWithExceptionInUserCode(MessageSender xferPublisher, BrokeredMessage message)
        {
            Console.WriteLine("Sending message with volatile with exception in user code");

            using (var scope = new TransactionScope(TransactionScopeOption.Required))
            {
                using (var conn = new SqlConnection(sqlConnectionString))
                {
                    conn.Open();

                    Transaction.Current.EnlistVolatile(new SendResourceManager(() => xferPublisher.Send(message)),
                        EnlistmentOptions.None);

                    throw new Exception("Will this roll back?"); // yes, no send happened

                    scope.Complete();
                }
            }
        }

        private static void SendSyncVolatileWithExceptionOnSend(MessageSender xferPublisher, BrokeredMessage message)
        {
            Console.WriteLine("Sending message with volatile with exception on send");

            using (var scope = new TransactionScope(TransactionScopeOption.Required))
            {
                using (var conn = new SqlConnection(sqlConnectionString))
                {
                    conn.Open();

                    Transaction.Current.EnlistVolatile(new SendResourceManager(() =>
                    {
                        // happens on background thread, so needs catching or you kill the process
                        try
                        {
                            throw new Exception("Will this roll back original?"); // nope
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine("Exception on background thread: " + ex);
                        }

                        
                    }), EnlistmentOptions.None);


                    scope.Complete();
                }
            }
        }
    }

    class SendResourceManager : IEnlistmentNotification
    {
        Action onCommit;

        public SendResourceManager(Action onCommit)
        {
            this.onCommit = onCommit;
        }

        public void Prepare(PreparingEnlistment preparingEnlistment)
        {
            preparingEnlistment.Prepared();
        }

        public void Commit(Enlistment enlistment)
        {
            Console.WriteLine("Comitting");

            onCommit();
            
            enlistment.Done();
        }

        public void Rollback(Enlistment enlistment)
        {
            Console.WriteLine("Rolling back");

            enlistment.Done();
        }

        public void InDoubt(Enlistment enlistment)
        {
            enlistment.Done();
        }

    }
}
