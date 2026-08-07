// Copyright 2021 - Haga Rakotoharivelo - https://github.com/haga-rak

using System;
using System.Collections.Concurrent;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Fluxzy.Core;
using Fluxzy.Rules.Actions;
using Fluxzy.Tests._Fixtures;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Xunit;

namespace Fluxzy.Tests.UnitTests.Core
{
    public class ResponseHeaderKeepAliveTests
    {
        [Fact]
        public void Response_Without_KeepAlive_Has_No_Protocol_Idle_Timeout()
        {
            var header = Parse("Content-Length: 2\r\n");

            Assert.Equal(-1, header.TimeoutIdleSeconds);
            Assert.Equal(-1, header.MaxConnection);
            Assert.False(header.ConnectionCloseRequest);
        }

        [Fact]
        public void Http10_Response_Without_KeepAlive_Requests_Connection_Close()
        {
            var header = Parse("Content-Length: 2\r\n", "HTTP/1.0");

            Assert.True(header.ConnectionCloseRequest);
        }

        [Fact]
        public void Http10_Response_With_KeepAlive_Can_Be_Reused()
        {
            var header = Parse(
                "Connection: keep-alive\r\nContent-Length: 2\r\n",
                "HTTP/1.0");

            Assert.False(header.ConnectionCloseRequest);
            Assert.Equal(-1, header.TimeoutIdleSeconds);
        }

        [Fact]
        public void Explicit_KeepAlive_Timeout_And_Max_Are_Parsed()
        {
            var header = Parse(
                "Connection: keep-alive\r\n" +
                "Keep-Alive: timeout=9, max=7\r\n");

            Assert.Equal(9, header.TimeoutIdleSeconds);
            Assert.Equal(7, header.MaxConnection);
            Assert.False(header.ConnectionCloseRequest);
        }

        [Fact]
        public void KeepAlive_Max_One_Requests_Connection_Close()
        {
            var header = Parse(
                "Connection: keep-alive\r\n" +
                "Keep-Alive: timeout=30, max=1\r\n");

            Assert.Equal(30, header.TimeoutIdleSeconds);
            Assert.Equal(1, header.MaxConnection);
            Assert.True(header.ConnectionCloseRequest);
        }

        [Fact]
        public async Task Requests_Over_One_Second_Apart_Reuse_Upstream_Tls_Connection()
        {
            var connectionIds = new ConcurrentDictionary<string, byte>();
            await using var origin = await InProcessHost.Create(app =>
                app.MapGet("/", (HttpContext context) => {
                    var connectionId = context.Connection.Id;
                    connectionIds.TryAdd(connectionId, 0);
                    context.Response.Headers["X-Connection-Id"] = connectionId;
                    return Results.Text("ok");
                }), suppressLogging: true, protocols: HttpProtocols.Http1);

            var setting = FluxzySetting.CreateLocalRandomPort();
            setting.ConfigureRule().WhenAny()
                   .Do(new SkipRemoteCertificateValidationAction());

            await using var proxy = new Proxy(setting);
            using var client = HttpClientUtility.CreateHttpClient(proxy.Run(), setting,
                handler => handler.ServerCertificateCustomValidationCallback =
                    HttpClientHandler.DangerousAcceptAnyServerCertificateValidator);
            client.DefaultRequestVersion = HttpVersion.Version11;
            client.DefaultVersionPolicy = HttpVersionPolicy.RequestVersionExact;

            var url = $"https://localhost:{origin.Port}/";
            var firstConnection = await GetConnectionId(client, url);

            await Task.Delay(TimeSpan.FromMilliseconds(1250));

            var secondConnection = await GetConnectionId(client, url);

            Assert.Equal(firstConnection, secondConnection);
            Assert.Single(connectionIds);
        }

        [Fact]
        public async Task Http10_Response_Without_KeepAlive_Uses_A_New_Upstream_Tls_Connection()
        {
            await using var origin = Http10TlsOrigin.Start();
            var setting = FluxzySetting.CreateLocalRandomPort();
            setting.ConfigureRule().WhenAny()
                   .Do(new SkipRemoteCertificateValidationAction());

            await using var proxy = new Proxy(setting);
            using var client = HttpClientUtility.CreateHttpClient(proxy.Run(), setting,
                handler => handler.ServerCertificateCustomValidationCallback =
                    HttpClientHandler.DangerousAcceptAnyServerCertificateValidator);
            client.DefaultRequestVersion = HttpVersion.Version11;
            client.DefaultVersionPolicy = HttpVersionPolicy.RequestVersionExact;

            var url = $"https://localhost:{origin.Port}/";
            var firstConnection = await GetConnectionId(client, url);
            var secondConnection = await GetConnectionId(client, url);

            Assert.NotEqual(firstConnection, secondConnection);
            Assert.Equal(2, origin.AcceptedConnections);
        }

        private static ResponseHeader Parse(string fields, string protocol = "HTTP/1.1") =>
            new($"{protocol} 200 OK\r\n{fields}\r\n".AsMemory(), true, true);

        private static async Task<string> GetConnectionId(HttpClient client, string url)
        {
            using var response = await client.GetAsync(url);

            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            Assert.Equal("ok", await response.Content.ReadAsStringAsync());

            return response.Headers.GetValues("X-Connection-Id").Single();
        }

        private sealed class Http10TlsOrigin : IAsyncDisposable
        {
            private readonly TcpListener _listener;
            private readonly System.Security.Cryptography.X509Certificates.X509Certificate2 _certificate;
            private readonly CancellationTokenSource _cts = new();
            private readonly ConcurrentBag<Task> _connections = new();
            private readonly Task _acceptLoop;
            private int _acceptedConnections;

            private Http10TlsOrigin(
                TcpListener listener,
                System.Security.Cryptography.X509Certificates.X509Certificate2 certificate)
            {
                _listener = listener;
                _certificate = certificate;
                _acceptLoop = AcceptLoop();
            }

            public int Port => ((IPEndPoint) _listener.LocalEndpoint).Port;

            public int AcceptedConnections => Volatile.Read(ref _acceptedConnections);

            public static Http10TlsOrigin Start()
            {
                var listener = new TcpListener(IPAddress.Loopback, 0);
                listener.Start();
                return new Http10TlsOrigin(
                    listener, InProcessHost.CreateSelfSignedCertificateForTesting());
            }

            private async Task AcceptLoop()
            {
                try
                {
                    while (!_cts.IsCancellationRequested)
                    {
                        var client = await _listener.AcceptTcpClientAsync(_cts.Token);
                        var connectionId = Interlocked.Increment(ref _acceptedConnections);
                        _connections.Add(HandleConnection(client, connectionId));
                    }
                }
                catch (OperationCanceledException)
                {
                }
                catch (ObjectDisposedException)
                {
                }
                catch (SocketException) when (_cts.IsCancellationRequested)
                {
                }
            }

            private async Task HandleConnection(TcpClient client, int connectionId)
            {
                try
                {
                    using var _ = client;
                    await using var stream = new SslStream(client.GetStream(), leaveInnerStreamOpen: false);
                    await stream.AuthenticateAsServerAsync(
                        _certificate,
                        clientCertificateRequired: false,
                        enabledSslProtocols: SslProtocols.Tls12,
                        checkCertificateRevocation: false);

                    var buffer = new byte[8192];
                    var pending = new StringBuilder();
                    var response = Encoding.ASCII.GetBytes(
                        "HTTP/1.0 200 OK\r\n" +
                        "Content-Length: 2\r\n" +
                        $"X-Connection-Id: {connectionId}\r\n\r\n" +
                        "ok");

                    while (!_cts.IsCancellationRequested)
                    {
                        var read = await stream.ReadAsync(buffer, _cts.Token);
                        if (read == 0)
                            return;

                        pending.Append(Encoding.ASCII.GetString(buffer, 0, read));
                        while (true)
                        {
                            var end = pending.ToString().IndexOf("\r\n\r\n", StringComparison.Ordinal);
                            if (end < 0)
                                break;

                            pending.Remove(0, end + 4);
                            await stream.WriteAsync(response, _cts.Token);
                            await stream.FlushAsync(_cts.Token);
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                }
                catch (IOException)
                {
                }
                catch (AuthenticationException)
                {
                }
            }

            public async ValueTask DisposeAsync()
            {
                _cts.Cancel();
                _listener.Stop();
                await _acceptLoop;
                await Task.WhenAll(_connections.ToArray());
                _certificate.Dispose();
                _cts.Dispose();
            }
        }
    }
}
