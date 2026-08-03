using System;
using System.IO;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Fluxzy.Clients;
using Fluxzy.Clients.H11;
using Fluxzy.Core;
using Fluxzy.Misc.ResizableBuffers;
using Fluxzy.Misc.Streams;
using Xunit;

namespace Fluxzy.Tests.Cases
{
    public class Http11PartialBodyDispositionTests
    {
        private static readonly Authority Authority = new("test.local", 80, false);

        [Theory]
        [InlineData("HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\n")]
        [InlineData("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n")]
        public async Task DisposingPartialCapturedBody_ClosesRatherThanRecycles(string response)
        {
            var connection = CreateConnection(response);
            var transport = (ScriptedDuplexStream)connection.ReadStream!;
            var pool = CreatePool();
            Enqueue(pool, connection);
            var exchange = CreateExchange();

            using (var buffer = RsBuffer.Allocate(256))
            using (var scope = new ExchangeScope())
                await pool.Send(exchange, null!, buffer, scope, CancellationToken.None);

            await using (var dispatch = new DispatchStream(
                exchange.Response.Body!, closeOnDone: true,
                DispatchStreamOwnership.OwnBaseStream, Stream.Null))
                exchange.Response.Body = dispatch;

            Assert.True(await exchange.Complete.WaitAsync(TimeSpan.FromSeconds(5)));
            await exchange.ConnectionDisposition.WaitAsync(TimeSpan.FromSeconds(5));

            Assert.Null(pool.DequeueReusableConnection(DateTime.UtcNow));
            Assert.True(transport.DisposeCount > 0);

            await pool.DisposeAsync();
        }

        private static Exchange CreateExchange() =>
            new(
                IIdProvider.FromZero,
                Authority,
                "GET / HTTP/1.1\r\nHost: test.local\r\n\r\n".AsMemory(),
                "HTTP/1.1",
                DateTime.UtcNow);

        private static Connection CreateConnection(string response)
        {
            var transport = new ScriptedDuplexStream(response);

            return new Connection(Authority, IIdProvider.FromZero)
            {
                ReadStream = transport,
                WriteStream = transport,
                UnderlyingTransport = transport
            };
        }

        private static Http11ConnectionPool CreatePool()
        {
            var runtimeSetting = ProxyRuntimeSetting.CreateDefault;
            runtimeSetting.ConcurrentConnection = 1;

            return new Http11ConnectionPool(
                Authority,
                remoteConnectionBuilder: null!,
                ITimingProvider.Default,
                runtimeSetting,
                archiveWriter: null!,
                resolutionResult: default);
        }

        private static void Enqueue(Http11ConnectionPool pool, Connection connection)
        {
            var field = typeof(Http11ConnectionPool).GetField(
                "_pendingConnections", BindingFlags.Instance | BindingFlags.NonPublic);
            var channel = (Channel<Http11ProcessingState>)field!.GetValue(pool)!;
            Assert.True(channel.Writer.TryWrite(
                new Http11ProcessingState(connection, DateTime.UtcNow)));
        }

        private sealed class ScriptedDuplexStream : Stream
        {
            private readonly byte[] _response;
            private int _offset;
            private int _disposeCount;

            public ScriptedDuplexStream(string response)
            {
                _response = Encoding.ASCII.GetBytes(response);
            }

            public int DisposeCount => Volatile.Read(ref _disposeCount);
            public override bool CanRead => true;
            public override bool CanSeek => false;
            public override bool CanWrite => true;
            public override long Length => throw new NotSupportedException();
            public override long Position
            {
                get => throw new NotSupportedException();
                set => throw new NotSupportedException();
            }

            public override ValueTask<int> ReadAsync(
                Memory<byte> buffer, CancellationToken cancellationToken = default)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var read = Math.Min(buffer.Length, _response.Length - _offset);

                if (read == 0)
                    return ValueTask.FromResult(0);

                _response.AsMemory(_offset, read).CopyTo(buffer);
                _offset += read;
                return ValueTask.FromResult(read);
            }

            public override ValueTask WriteAsync(
                ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
            {
                cancellationToken.ThrowIfCancellationRequested();
                return ValueTask.CompletedTask;
            }

            protected override void Dispose(bool disposing)
            {
                if (disposing)
                    Interlocked.Increment(ref _disposeCount);

                base.Dispose(disposing);
            }

            public override void Flush()
            {
            }

            public override int Read(byte[] buffer, int offset, int count) =>
                throw new NotSupportedException();
            public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
            public override void SetLength(long value) => throw new NotSupportedException();
            public override void Write(byte[] buffer, int offset, int count)
            {
            }
        }
    }
}
