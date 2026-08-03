using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Fluxzy.Misc.Streams;
using Xunit;

namespace Fluxzy.Tests.UnitTests.Misc
{
    public class RecomposedStreamTests
    {
        [Fact]
        public void Flush_OnlyFlushesWriteStream()
        {
            using var readStream = new FlushTrackingStream();
            using var writeStream = new FlushTrackingStream();
            using var stream = new RecomposedStream(readStream, writeStream);

            stream.Flush();

            Assert.Equal(0, readStream.SyncFlushCount);
            Assert.Equal(1, writeStream.SyncFlushCount);
        }

        [Fact]
        public async Task FlushAsync_OnlyFlushesWriteStreamAndWaitsForCompletion()
        {
            var writeCompletion = new TaskCompletionSource<bool>(
                TaskCreationOptions.RunContinuationsAsynchronously);
            using var readStream = new FlushTrackingStream();
            using var writeStream = new FlushTrackingStream(writeCompletion.Task);
            using var stream = new RecomposedStream(readStream, writeStream);

            var flushTask = stream.FlushAsync(CancellationToken.None);

            Assert.Same(writeCompletion.Task, flushTask);
            Assert.Equal(0, readStream.AsyncFlushCount);
            Assert.Equal(1, writeStream.AsyncFlushCount);
            Assert.False(flushTask.IsCompleted);

            writeCompletion.SetResult(true);
            await flushTask;
        }

        private sealed class FlushTrackingStream : MemoryStream
        {
            private readonly Task _flushCompletion;

            public FlushTrackingStream(Task? flushCompletion = null)
            {
                _flushCompletion = flushCompletion ?? Task.CompletedTask;
            }

            public int SyncFlushCount { get; private set; }

            public int AsyncFlushCount { get; private set; }

            public override void Flush()
            {
                SyncFlushCount++;
            }

            public override Task FlushAsync(CancellationToken cancellationToken)
            {
                AsyncFlushCount++;
                return _flushCompletion;
            }
        }
    }
}
