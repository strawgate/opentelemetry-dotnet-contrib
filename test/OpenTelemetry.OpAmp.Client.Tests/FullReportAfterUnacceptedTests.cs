// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#if NETFRAMEWORK
using System.Net.Http;
#endif
using System.Collections.Concurrent;
using System.Net;
using Google.Protobuf;
using OpAmp.Proto.V1;
using OpenTelemetry.OpAmp.Client.Internal;
using OpenTelemetry.OpAmp.Client.Internal.Transport.Http;
using OpenTelemetry.OpAmp.Client.Settings;

namespace OpenTelemetry.OpAmp.Client.Tests;

public class FullReportAfterUnacceptedTests
{
    [Theory]
    [InlineData(HttpStatusCode.ServiceUnavailable)]
    [InlineData(HttpStatusCode.InternalServerError)]
    public async Task OpAmpPipe_SendsFullIdentificationAfterAnUnacceptedMessage(HttpStatusCode firstStatus)
    {
        var handler = new RecordingHandler(firstStatus);
        using var pipe = CreatePipe(handler);

        OpAmpPipeTests.AppendIdentification(pipe);
        await handler.WaitForRequestsAsync(1);

        // Nothing extra goes out because of the failure...
        await Task.Delay(TimeSpan.FromMilliseconds(300));
        Assert.Single(handler.Requests);

        // ...but the next message carries the full identification.
        OpAmpPipeTests.AppendHeartbeat(pipe);
        await handler.WaitForRequestsAsync(2);
        var next = handler.Requests.ElementAt(1);
        Assert.NotNull(next.AgentDescription);
        Assert.NotNull(next.Health);
    }

    [Fact]
    public async Task OpAmpPipe_DoesNotResendARejectedIdentification()
    {
        var handler = new RecordingHandler(HttpStatusCode.RequestEntityTooLarge);
        using var pipe = CreatePipe(handler);

        OpAmpPipeTests.AppendIdentification(pipe);
        await handler.WaitForRequestsAsync(1);

        OpAmpPipeTests.AppendHeartbeat(pipe);
        await handler.WaitForRequestsAsync(2);
        var next = handler.Requests.ElementAt(1);
        Assert.Null(next.AgentDescription);
        Assert.NotNull(next.Health);
    }

    private static OpAmpPipe CreatePipe(RecordingHandler handler)
    {
        var settings = new OpAmpClientSettings
        {
            ServerUrl = new Uri("http://localhost/v1/opamp"),
            HttpClientFactory = () => new HttpClient(handler),
        };
        var processor = new FrameProcessor();

        return new OpAmpPipe(settings, processor, new PlainHttpTransport(settings, processor));
    }

    // Answers the first request with `firstStatus`, later ones with an empty ServerToAgent.
    private sealed class RecordingHandler(HttpStatusCode firstStatus) : HttpMessageHandler
    {
        private readonly SemaphoreSlim received = new(0);

        public ConcurrentQueue<AgentToServer> Requests { get; } = new();

        public async Task WaitForRequestsAsync(int count)
        {
            while (this.Requests.Count < count)
            {
                Assert.True(await this.received.WaitAsync(TimeSpan.FromSeconds(10)), $"fewer than {count} requests");
            }
        }

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
#if NET
            var body = await request.Content!.ReadAsByteArrayAsync(cancellationToken).ConfigureAwait(false);
#else
            var body = await request.Content!.ReadAsByteArrayAsync().ConfigureAwait(false);
#endif
            this.Requests.Enqueue(AgentToServer.Parser.ParseFrom(body));
            var first = this.Requests.Count == 1;
            this.received.Release();

            return first
                ? new HttpResponseMessage(firstStatus)
                : new HttpResponseMessage(HttpStatusCode.OK) { Content = new ByteArrayContent(new ServerToAgent().ToByteArray()) };
        }
    }
}
