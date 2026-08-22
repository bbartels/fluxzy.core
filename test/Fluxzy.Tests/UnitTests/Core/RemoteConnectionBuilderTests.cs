// Copyright 2021 - Haga Rakotoharivelo - https://github.com/haga-rak

using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Fluxzy.Clients;
using Fluxzy.Clients.Ssl;
using Fluxzy.Core;
using Fluxzy.Misc.Streams;
using Xunit;

namespace Fluxzy.Tests.UnitTests.Core
{
    public class RemoteConnectionBuilderTests
    {
        [Fact]
        public async Task TlsFailureDisposesOpenedStreamAndClearsConnection()
        {
            var listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();

            using var client = new TcpClient();
            var endpoint = (IPEndPoint) listener.LocalEndpoint;
            var acceptTask = listener.AcceptTcpClientAsync();
            await client.ConnectAsync(endpoint.Address, endpoint.Port);
            using var peer = await acceptTask;
            listener.Stop();

            var disposeCount = 0;
            var stream = new DisposeEventNotifierStream(
                client, () => {
                    Interlocked.Increment(ref disposeCount);
                    return ValueTask.CompletedTask;
                });
            var expected = new InvalidDataException("TLS failed");
            var builder = new RemoteConnectionBuilder(
                ITimingProvider.Default, new FailingSslConnectionBuilder(expected));
            var setting = ProxyRuntimeSetting.CreateDefault;
            setting.TcpConnectionProvider = new TestTcpConnectionProvider(stream);
            var exchange = new Exchange(
                IIdProvider.FromZero,
                new Authority("example.com", endpoint.Port, true),
                "GET / HTTP/1.1\r\nHost: example.com\r\n\r\n".AsMemory(),
                "HTTP/1.1",
                DateTime.UtcNow);

            var actual = await Assert.ThrowsAsync<InvalidDataException>(() =>
                builder.OpenConnectionToRemote(
                    exchange,
                    new DnsResolutionResult(endpoint, DateTime.UtcNow, DateTime.UtcNow),
                    new List<SslApplicationProtocol> { SslApplicationProtocol.Http11 },
                    setting,
                    null,
                    CancellationToken.None).AsTask());

            Assert.Same(expected, actual);
            Assert.Equal(1, disposeCount);
            Assert.Null(exchange.Connection);
        }

        private sealed class FailingSslConnectionBuilder : ISslConnectionBuilder
        {
            private readonly Exception _exception;

            public FailingSslConnectionBuilder(Exception exception)
            {
                _exception = exception;
            }

            public Task<SslConnection> AuthenticateAsClient(
                Stream innerStream,
                SslConnectionBuilderOptions sslOptions,
                Action<string> onKeyReceived,
                CancellationToken token)
            {
                return Task.FromException<SslConnection>(_exception);
            }
        }

        private sealed class TestTcpConnectionProvider : ITcpConnectionProvider
        {
            private readonly DisposeEventNotifierStream _stream;

            public TestTcpConnectionProvider(DisposeEventNotifierStream stream)
            {
                _stream = stream;
            }

            public ITcpConnection Create(string dumpFileName)
            {
                return new TestTcpConnection(_stream);
            }

            public void TryFlush()
            {
            }

            public ValueTask DisposeAsync()
            {
                return ValueTask.CompletedTask;
            }
        }

        private sealed class TestTcpConnection : ITcpConnection
        {
            private readonly ITcpConnectionConnectResult _result;

            public TestTcpConnection(DisposeEventNotifierStream stream)
            {
                _result = new TestTcpConnectionConnectResult(stream);
            }

            public Task<ITcpConnectionConnectResult> ConnectAsync(IPAddress address, int port)
            {
                return Task.FromResult(_result);
            }

            public ValueTask DisposeAsync()
            {
                return ValueTask.CompletedTask;
            }
        }

        private sealed class TestTcpConnectionConnectResult : ITcpConnectionConnectResult
        {
            public TestTcpConnectionConnectResult(DisposeEventNotifierStream stream)
            {
                Stream = stream;
            }

            public DisposeEventNotifierStream Stream { get; }

            public void ProcessNssKey(string nssKey)
            {
            }
        }
    }
}
