// Copyright 2021 - Haga Rakotoharivelo - https://github.com/haga-rak

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Json;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Fluxzy.Rules.Actions;
using Fluxzy.Rules.Filters;
using Fluxzy.Tests._Fixtures;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Xunit;

namespace Fluxzy.Tests.Cases;

/// <summary>
///     Request bodies must survive a protocol change between the client and the origin,
///     including bodies whose length is not declared up front (HTTP/2 without content-length,
///     HTTP/1.1 chunked), such as a large `git push`.
/// </summary>
public class CrossProtocolRequestBodyTests
{
    [Theory]
    [InlineData(1, false)]
    [InlineData(64 * 1024 + 1, false)]
    [InlineData(1024 * 1024 + 1, false)]
    [InlineData(1024 * 1024 + 1, true)]
    public async Task H2_Client_Upload_Reaches_H11_Only_Origin(int length, bool declareLength)
    {
        await using var origin = await CreateOrigin(HttpProtocols.Http1);
        await using var proxy = CreateProxy();
        using var client = CreateClient(proxy.Run().First(), origin, new Version(2, 0));

        await using var body = new RandomDataStream(9, length);
        var content = CreateContent(body, length, declareLength);

        var response = await client.PostAsync("/upload", content);
        var received = await ReadReceived(response);

        Assert.Equal(new Version(2, 0), response.Version);
        Assert.Equal("HTTP/1.1", received.Protocol);
        Assert.Equal(length, received.Length);
        Assert.Equal(body.Hash, received.Sha1);

        if (declareLength)
            Assert.Equal(length, received.ContentLength);
        else
            Assert.Equal("chunked", received.TransferEncoding);
    }

    [Fact]
    public async Task H2_Client_Bodyless_Request_Is_Not_Chunked_For_H11_Origin()
    {
        await using var origin = await CreateOrigin(HttpProtocols.Http1);
        await using var proxy = CreateProxy();
        using var client = CreateClient(proxy.Run().First(), origin, new Version(2, 0));

        var response = await client.GetAsync("/upload");
        var received = await ReadReceived(response);

        Assert.Equal("HTTP/1.1", received.Protocol);
        Assert.Equal(0, received.Length);
        Assert.Equal(string.Empty, received.TransferEncoding);
        Assert.Null(received.ContentLength);
    }

    [Theory]
    [InlineData(1, false)]
    [InlineData(64 * 1024 + 1, false)]
    [InlineData(1024 * 1024 + 1, false)]
    [InlineData(1024 * 1024 + 1, true)]
    public async Task H11_Client_Upload_Reaches_H2_Only_Origin(int length, bool declareLength)
    {
        await using var origin = await CreateOrigin(HttpProtocols.Http2);
        await using var proxy = CreateProxy();
        using var client = CreateClient(proxy.Run().First(), origin, new Version(1, 1));

        await using var body = new RandomDataStream(9, length);
        var content = CreateContent(body, length, declareLength);

        var response = await client.PostAsync("/upload", content);
        var received = await ReadReceived(response);

        Assert.Equal(new Version(1, 1), response.Version);
        Assert.Equal("HTTP/2", received.Protocol);
        Assert.Equal(length, received.Length);
        Assert.Equal(body.Hash, received.Sha1);
        Assert.Equal(string.Empty, received.TransferEncoding);
    }

    [Theory]
    [InlineData("Transfer-Encoding: gzip, chunked")]
    [InlineData("Transfer-Encoding: chunked,")]
    [InlineData("Transfer-Encoding: chunked, chunked")]
    [InlineData("Transfer-Encoding: chunked\r\nTransfer-Encoding: chunked")]
    public async Task H11_Client_Transfer_Coding_H2_Cannot_Carry_Is_Refused(string transferEncodingFields)
    {
        var originRequests = 0;
        await using var origin = await CreateOrigin(HttpProtocols.Http2, () => Interlocked.Increment(ref originRequests));
        await using var proxy = CreateProxy();
        var proxyEndPoint = proxy.Run().First();
        using var client = CreateClient(proxyEndPoint, origin, new Version(1, 1));

        var before = await Upload(client);

        // Sent verbatim (HttpClient would normalise the fields). Only one layer of recognised
        // chunked framing can be removed before HTTP/2: "chunked," is not recognised so its
        // body is never read, and the others leave a coding HTTP/2 cannot announce.
        var refused = await SendRawChunkedPost(proxyEndPoint, origin.Port, transferEncodingFields);

        Assert.StartsWith("HTTP/1.1 528 ", refused);
        Assert.Contains("x-fluxzy-network-error: protocol_error\r\n", refused);
        Assert.Equal(1, Volatile.Read(ref originRequests));

        // Only the offending request is refused: the next upload reuses the same HTTP/2
        // connection to the origin instead of a replacement opened after a pool teardown.
        var after = await Upload(client);

        Assert.Equal("HTTP/2", after.Protocol);
        Assert.Equal(before.ConnectionId, after.ConnectionId);
        Assert.Equal(2, Volatile.Read(ref originRequests));
    }

    private static async Task<string> SendRawChunkedPost(
        IPEndPoint proxyEndPoint, int originPort, string transferEncodingFields)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        var token = timeout.Token;

        using var tcpClient = new TcpClient();
        await tcpClient.ConnectAsync(proxyEndPoint, token);

        var tunnel = tcpClient.GetStream();
        var authority = $"localhost:{originPort}";
        await tunnel.WriteAsync(Encoding.ASCII.GetBytes($"CONNECT {authority} HTTP/1.1\r\nHost: {authority}\r\n\r\n"), token);
        await ReadHeaderBlock(tunnel, token);

        await using var tls = new SslStream(tunnel, false, (_, _, _, _) => true);
        await tls.AuthenticateAsClientAsync(new SslClientAuthenticationOptions {
            TargetHost = "localhost",
            ApplicationProtocols = new List<SslApplicationProtocol> { SslApplicationProtocol.Http11 }
        }, token);

        await tls.WriteAsync(Encoding.ASCII.GetBytes(
            $"POST /upload HTTP/1.1\r\nHost: {authority}\r\n{transferEncodingFields}\r\n" +
            "Connection: close\r\n\r\n5\r\nhello\r\n0\r\n\r\n"), token);

        return await ReadHeaderBlock(tls, token);
    }

    private static async Task<string> ReadHeaderBlock(Stream stream, CancellationToken token)
    {
        var received = new StringBuilder();
        var buffer = new byte[1];

        while (!received.ToString().EndsWith("\r\n\r\n", StringComparison.Ordinal)
               && await stream.ReadAsync(buffer, token) == 1) {
            received.Append((char) buffer[0]);
        }

        return received.ToString();
    }

    private static async Task<ReceivedBody> Upload(HttpClient client)
    {
        await using var body = new RandomDataStream(9, 1024);
        var received = await ReadReceived(await client.PostAsync("/upload", new StreamContent(body)));

        Assert.Equal(body.Hash, received.Sha1);

        return received;
    }

    private static Task<InProcessHost> CreateOrigin(HttpProtocols protocols, Action? onRequest = null)
    {
        return InProcessHost.Create(app => app.Map("/upload", async (HttpContext ctx) => {
            onRequest?.Invoke();

            using var sha1 = SHA1.Create();
            var buffer = new byte[16 * 1024];
            long total = 0;
            int read;

            while ((read = await ctx.Request.Body.ReadAsync(buffer)) > 0) {
                sha1.TransformBlock(buffer, 0, read, null, 0);
                total += read;
            }

            sha1.TransformFinalBlock(Array.Empty<byte>(), 0, 0);

            return Results.Json(new ReceivedBody(
                ctx.Request.Protocol, total, Convert.ToHexString(sha1.Hash!),
                ctx.Request.Headers.TransferEncoding.ToString(), ctx.Request.ContentLength,
                ctx.Connection.Id));
        }), suppressLogging: true, protocols: protocols);
    }

    private static Proxy CreateProxy()
    {
        var setting = FluxzySetting.CreateLocalRandomPort();
        setting.SetServeH2(true);
        setting.AddAlterationRules(new SkipRemoteCertificateValidationAction(), AnyFilter.Default);

        return new Proxy(setting);
    }

    private static HttpClient CreateClient(IPEndPoint proxyEndPoint, InProcessHost origin, Version httpVersion)
    {
        var client = Socks5ClientFactory.Create(proxyEndPoint, httpVersion: httpVersion);
        client.BaseAddress = new Uri(origin.BaseUrl);

        return client;
    }

    private static HttpContent CreateContent(RandomDataStream body, int length, bool declareLength)
    {
        var content = new StreamContent(body);

        // A non-seekable stream leaves the length unknown: HttpClient then streams the body
        // (no content-length on HTTP/2, chunked on HTTP/1.1).
        if (declareLength)
            content.Headers.ContentLength = length;

        return content;
    }

    private static async Task<ReceivedBody> ReadReceived(HttpResponseMessage response)
    {
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);

        return (await response.Content.ReadFromJsonAsync<ReceivedBody>())!;
    }

    private sealed record ReceivedBody(
        string Protocol, long Length, string Sha1, string TransferEncoding, long? ContentLength,
        string ConnectionId);
}
