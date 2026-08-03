// Copyright 2021 - Haga Rakotoharivelo - https://github.com/haga-rak

using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Fluxzy.Clients;
using Fluxzy.Clients.H11;
using Fluxzy.Core;
using Fluxzy.Misc.Streams;
using Xunit;
using ResizableBuffer = Fluxzy.Misc.ResizableBuffers.RsBuffer;

namespace Fluxzy.Tests.UnitTests.H11Client
{
    public class Http11PoolProcessingPushbackTests
    {
        private static readonly Http11PoolProcessing Processor = new(
            TimeSpan.Zero, Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);

        [Fact]
        public async Task HeaderOvershoot_PrependsBodyBytesExactlyOnce()
        {
            var readStream = new TrackingStream(
                "HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nab",
                "cde");
            var connection = CreateConnection(readStream, new TrackingStream(canWrite: true));

            var exchange = await Process(connection);

            Assert.Equal("abcde", await ReadBody(exchange));
            var pushback = Assert.IsType<PushbackReadStream>(connection.ReadStream);
            Assert.Equal(0, pushback.PendingLength);
        }

        [Fact]
        public async Task HeaderOvershoot_RetainsPipelinedResponseAndReusesWrapper()
        {
            var readStream = new TrackingStream(
                "HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\none" +
                "HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\ntwo");
            var connection = CreateConnection(readStream, new TrackingStream(canWrite: true));

            var first = await Process(connection);
            var wrapper = Assert.IsType<PushbackReadStream>(connection.ReadStream);

            Assert.Equal("one", await ReadBody(first));

            var second = await Process(connection);

            Assert.Same(wrapper, connection.ReadStream);
            Assert.Equal("two", await ReadBody(second));
            Assert.Equal(0, wrapper.PendingLength);
        }

        [Fact]
        public async Task HeaderOvershoot_RetainsPipelinedResponseAfterEmptyBody()
        {
            var readStream = new TrackingStream(
                "HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n" +
                "HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\ntwo");
            var connection = CreateConnection(readStream, new TrackingStream(canWrite: true));

            var first = await Process(connection);
            var wrapper = Assert.IsType<PushbackReadStream>(connection.ReadStream);

            Assert.Same(Stream.Null, first.Response.Body);

            var second = await Process(connection);

            Assert.Same(wrapper, connection.ReadStream);
            Assert.Equal("two", await ReadBody(second));
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task PoolDisposal_ClosesSharedAndDistinctTransportsExactlyOnce(bool sharedTransport)
        {
            var readStream = new TrackingStream(
                new[] { "HTTP/1.1 200 OK\r\nContent-Length: 1\r\n\r\nx" },
                canWrite: sharedTransport);
            var writeStream = sharedTransport ? readStream : new TrackingStream(canWrite: true);
            var connection = CreateConnection(readStream, writeStream);

            var exchange = await Process(connection);
            Assert.Equal("x", await ReadBody(exchange));
            Assert.IsType<PushbackReadStream>(connection.ReadStream);

            var pool = CreatePool();
            Assert.True(Enqueue(pool, connection));

            await pool.DisposeAsync();

            Assert.Equal(1, readStream.DisposeCount);
            Assert.Equal(1, writeStream.DisposeCount);
        }

        [SuppressMessage("Reliability", "CA2022:Avoid inexact read",
            Justification = "The partial read is the behavior under test.")]
        [Fact]
        public async Task PartialOvershoot_IsDeliveredBeforeReadFault()
        {
            var expected = new IOException("body failed");
            var readStream = new TrackingStream(
                new[] { "HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nab" },
                terminalException: expected);
            var connection = CreateConnection(readStream, new TrackingStream(canWrite: true));
            var exchange = await Process(connection);
            var buffer = new byte[8];

            var read = await exchange.Response.Body!.ReadAsync(buffer);

            Assert.Equal(2, read);
            Assert.Equal("ab", Encoding.ASCII.GetString(buffer, 0, read));
            var actual = await Assert.ThrowsAsync<IOException>(
                async () => await exchange.Response.Body.ReadAsync(buffer));
            Assert.Same(expected, actual);
            Assert.True(exchange.Complete.IsFaulted);
        }

        [SuppressMessage("Reliability", "CA2022:Avoid inexact read",
            Justification = "The partial read is the behavior under test.")]
        [Fact]
        public async Task PartialOvershoot_IsDeliveredBeforeReadCancellation()
        {
            using var cancellation = new CancellationTokenSource();
            var readStream = new TrackingStream(
                new[] { "HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nab" },
                waitForCancellation: true);
            var connection = CreateConnection(readStream, new TrackingStream(canWrite: true));
            var exchange = await Process(connection, cancellation.Token);
            var buffer = new byte[8];

            var read = await exchange.Response.Body!.ReadAsync(buffer);
            cancellation.Cancel();

            Assert.Equal(2, read);
            Assert.Equal("ab", Encoding.ASCII.GetString(buffer, 0, read));
            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                async () => await exchange.Response.Body.ReadAsync(buffer));
            Assert.True(exchange.Complete.IsFaulted);
        }

        private static async Task<Exchange> Process(
            Connection connection, CancellationToken cancellationToken = default)
        {
            var authority = connection.Authority;
            var exchange = new Exchange(
                IIdProvider.FromZero,
                authority,
                "GET / HTTP/1.1\r\nHost: test.local\r\n\r\n".AsMemory(),
                "HTTP/1.1",
                DateTime.UtcNow) {
                Connection = connection
            };

            using var buffer = ResizableBuffer.Allocate(256);
            using var scope = new ExchangeScope();
            await Processor.Process(exchange, buffer, scope, cancellationToken);
            return exchange;
        }

        private static async Task<string> ReadBody(Exchange exchange)
        {
            using var result = new MemoryStream();
            await exchange.Response.Body!.CopyToAsync(result);
            return Encoding.ASCII.GetString(result.ToArray());
        }

        private static Connection CreateConnection(Stream readStream, Stream writeStream)
        {
            var connection = new Connection(
                new Authority("test.local", 80, false), IIdProvider.FromZero) {
                ReadStream = readStream,
                WriteStream = writeStream
            };

            return connection;
        }

        private static Http11ConnectionPool CreatePool()
        {
            return new Http11ConnectionPool(
                new Authority("test.local", 80, false),
                remoteConnectionBuilder: null!,
                ITimingProvider.Default,
                ProxyRuntimeSetting.CreateDefault,
                archiveWriter: null!,
                resolutionResult: default,
                onConnectionFaulted: _ => { });
        }

        private static bool Enqueue(Http11ConnectionPool pool, Connection connection)
        {
            var field = typeof(Http11ConnectionPool).GetField(
                "_pendingConnections", BindingFlags.Instance | BindingFlags.NonPublic);
            var channel = (Channel<Http11ProcessingState>) field!.GetValue(pool)!;
            return channel.Writer.TryWrite(new Http11ProcessingState(connection, DateTime.UtcNow));
        }

        private sealed class TrackingStream : Stream
        {
            private readonly Queue<byte[]> _segments = new();
            private readonly Exception? _terminalException;
            private readonly bool _waitForCancellation;
            private readonly bool _canWrite;
            private int _segmentOffset;

            public TrackingStream(params string[] segments)
                : this(segments, false)
            {
            }

            public TrackingStream(bool canWrite)
                : this(Array.Empty<string>(), canWrite)
            {
            }

            public TrackingStream(
                IEnumerable<string> segments,
                bool canWrite = false,
                Exception? terminalException = null,
                bool waitForCancellation = false)
            {
                foreach (var segment in segments) {
                    _segments.Enqueue(Encoding.ASCII.GetBytes(segment));
                }

                _terminalException = terminalException;
                _waitForCancellation = waitForCancellation;
                _canWrite = canWrite;
            }

            public int DisposeCount { get; private set; }

            public override bool CanRead => true;
            public override bool CanSeek => false;
            public override bool CanWrite => _canWrite;
            public override long Length => throw new NotSupportedException();
            public override long Position {
                get => throw new NotSupportedException();
                set => throw new NotSupportedException();
            }

            public override int Read(byte[] buffer, int offset, int count)
            {
                return Read(buffer.AsSpan(offset, count));
            }

            public override int Read(Span<byte> buffer)
            {
                if (TryRead(buffer, out var read)) {
                    return read;
                }

                if (_terminalException != null) {
                    throw _terminalException;
                }

                return 0;
            }

            public override ValueTask<int> ReadAsync(
                Memory<byte> buffer, CancellationToken cancellationToken = default)
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (TryRead(buffer.Span, out var read)) {
                    return ValueTask.FromResult(read);
                }

                if (_terminalException != null) {
                    return ValueTask.FromException<int>(_terminalException);
                }

                return _waitForCancellation
                    ? WaitForCancellation(cancellationToken)
                    : ValueTask.FromResult(0);
            }

            private bool TryRead(Span<byte> destination, out int read)
            {
                if (_segments.Count == 0) {
                    read = 0;
                    return false;
                }

                var segment = _segments.Peek();
                read = Math.Min(destination.Length, segment.Length - _segmentOffset);
                segment.AsSpan(_segmentOffset, read).CopyTo(destination);
                _segmentOffset += read;

                if (_segmentOffset == segment.Length) {
                    _segments.Dequeue();
                    _segmentOffset = 0;
                }

                return true;
            }

            private static async ValueTask<int> WaitForCancellation(CancellationToken cancellationToken)
            {
                await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
                return 0;
            }

            public override void Write(byte[] buffer, int offset, int count)
            {
                if (!_canWrite) {
                    throw new NotSupportedException();
                }
            }

            public override ValueTask WriteAsync(
                ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (!_canWrite) {
                    return ValueTask.FromException(new NotSupportedException());
                }

                return ValueTask.CompletedTask;
            }

            protected override void Dispose(bool disposing)
            {
                if (disposing) {
                    DisposeCount++;
                }

                base.Dispose(disposing);
            }

            public override void Flush()
            {
            }

            public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

            public override void SetLength(long value) => throw new NotSupportedException();
        }
    }
}
