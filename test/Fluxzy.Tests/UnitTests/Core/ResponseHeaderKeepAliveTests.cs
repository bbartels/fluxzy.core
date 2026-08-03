// Copyright 2021 - Haga Rakotoharivelo - https://github.com/haga-rak

using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Net;
using System.Net.Http;
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

        private static ResponseHeader Parse(string fields) =>
            new($"HTTP/1.1 200 OK\r\n{fields}\r\n".AsMemory(), true, true);

        private static async Task<string> GetConnectionId(HttpClient client, string url)
        {
            using var response = await client.GetAsync(url);

            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            Assert.Equal("ok", await response.Content.ReadAsStringAsync());

            return response.Headers.GetValues("X-Connection-Id").Single();
        }
    }
}
