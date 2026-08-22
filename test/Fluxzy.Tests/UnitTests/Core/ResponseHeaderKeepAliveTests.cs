// Copyright 2021 - Haga Rakotoharivelo - https://github.com/haga-rak

using System;
using System.Linq;
using Fluxzy.Clients.H2.Encoder;
using Fluxzy.Core;
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
                "Connection: custom, keep-alive\r\n" +
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

        [Theory]
        [InlineData("Connection: close\r\n")]
        [InlineData("Connection: custom, close\r\n")]
        [InlineData("Connection: close, custom\r\n")]
        [InlineData("Connection: keep-alive, close\r\n")]
        [InlineData("Connection: custom, \tclose \r\n")]
        [InlineData("Connection: custom\r\nConnection: close\r\n")]
        public void Http11_Close_Connection_Option_Is_Recognized(string fields)
        {
            var header = Parse(fields);

            Assert.True(header.ConnectionCloseRequest);
        }

        [Fact]
        public void Switching_Protocols_Recognizes_Upgrade_In_Connection_Option_List()
        {
            var header = Parse("Connection: keep-alive, upgrade\r\n", statusCode: 101);

            Assert.True(header.ConnectionCloseRequest);
        }

        [Fact]
        public void Http10_Without_KeepAlive_Requests_Connection_Close()
        {
            var header = Parse("Content-Length: 2\r\n", protocol: "HTTP/1.0");

            Assert.True(header.ConnectionCloseRequest);
        }

        [Theory]
        [InlineData("Connection: keep-alive\r\nContent-Length: 2\r\n")]
        [InlineData("Connection: custom, keep-alive\r\nContent-Length: 2\r\n")]
        [InlineData("Connection: custom\r\nConnection: keep-alive\r\nContent-Length: 2\r\n")]
        public void Self_Delimited_Http10_KeepAlive_Can_Be_Reused(string fields)
        {
            var header = Parse(fields, protocol: "HTTP/1.0");

            Assert.False(header.ConnectionCloseRequest);
            Assert.Equal(-1, header.TimeoutIdleSeconds);
        }

        [Fact]
        public void Http10_KeepAlive_Without_Self_Delimited_Body_Requests_Close()
        {
            var header = Parse("Connection: keep-alive\r\n", protocol: "HTTP/1.0");

            Assert.True(header.ConnectionCloseRequest);
        }

        [Theory]
        [InlineData("HTTP/1.0")]
        [InlineData("HTTP/1.1")]
        public void Close_Delimited_Transfer_Encoding_Overrides_Content_Length(string protocol)
        {
            var header = Parse(
                "Connection: keep-alive\r\n" +
                "Transfer-Encoding: gzip\r\n" +
                "Content-Length: 2\r\n",
                protocol: protocol);

            Assert.True(header.ConnectionCloseRequest);
            Assert.Equal(-1, header.ContentLength);
            Assert.DoesNotContain(header.HeaderFields,
                field => field.Name.Span.Equals("content-length", StringComparison.OrdinalIgnoreCase));
        }

        [Theory]
        [InlineData(199)]
        [InlineData(204)]
        [InlineData(304)]
        public void Bodyless_Http10_KeepAlive_Can_Be_Reused(int statusCode)
        {
            var header = Parse("Connection: keep-alive\r\n", protocol: "HTTP/1.0", statusCode: statusCode);

            Assert.False(header.ConnectionCloseRequest);
        }

        [Fact]
        public void Unframed_Http10_205_Requests_Close()
        {
            var header = Parse("Connection: keep-alive\r\n", protocol: "HTTP/1.0", statusCode: 205);

            Assert.True(header.ConnectionCloseRequest);
        }

        [Fact]
        public void Explicitly_Empty_Http10_205_Can_Be_Reused()
        {
            var header = Parse(
                "Connection: keep-alive\r\nContent-Length: 0\r\n",
                protocol: "HTTP/1.0", statusCode: 205);

            Assert.False(header.ConnectionCloseRequest);
        }

        [Fact]
        public void Direct_H2_Header_Does_Not_Inherit_Http10_Close_By_Default()
        {
            var header = new ResponseHeader(new[] { new HeaderField(":status", "200") });

            Assert.False(header.ConnectionCloseRequest);
            Assert.Equal(-1, header.TimeoutIdleSeconds);
        }

        private static ResponseHeader Parse(
            string fields,
            string protocol = "HTTP/1.1",
            int statusCode = 200)
        {
            var rawHeader = $"{protocol} {statusCode} Test\r\n{fields}\r\n";

            return new ResponseHeader(rawHeader.AsMemory(), false, true);
        }
    }
}
