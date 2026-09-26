// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#if NETFRAMEWORK
using System.Net.Http;
#endif
using System.Diagnostics;
using System.Net;
using System.Net.Http.Headers;
using OpenTelemetry.OpAmp.Client.Internal;
using OpenTelemetry.OpAmp.Client.Internal.Transport.Http;
using OpenTelemetry.OpAmp.Client.Internal.Utils;
using OpenTelemetry.OpAmp.Client.Settings;
using OpenTelemetry.OpAmp.Client.Tests.Tools;

namespace OpenTelemetry.OpAmp.Client.Tests;

public class RetryAfterTests
{
    private static readonly DateTimeOffset Now = new(2026, 9, 26, 12, 0, 0, TimeSpan.Zero);

    [Fact]
    public void RetryAfter_ParsesSecondsAndDates()
    {
        Assert.Equal(TimeSpan.FromSeconds(4), RetryAfter.FromHeader(new RetryConditionHeaderValue(TimeSpan.FromSeconds(4)), Now));
        Assert.Equal(TimeSpan.FromSeconds(30), RetryAfter.FromHeader(new RetryConditionHeaderValue(Now.AddSeconds(30)), Now));

        // A date in the past means now; a wait over an hour is cut to an hour.
        Assert.Equal(TimeSpan.Zero, RetryAfter.FromHeader(new RetryConditionHeaderValue(Now.AddSeconds(-30)), Now));
        Assert.Equal(RetryAfter.Max, RetryAfter.FromHeader(new RetryConditionHeaderValue(TimeSpan.FromDays(1)), Now));
        Assert.Null(RetryAfter.FromHeader(null, Now));
    }

    [Fact]
    public void RetryAfter_ParsesRetryInfo()
    {
        Assert.Equal(TimeSpan.FromSeconds(2), RetryAfter.FromRetryInfo(2_000_000_000));
        Assert.Equal(RetryAfter.Max, RetryAfter.FromRetryInfo(ulong.MaxValue));
    }

    [Fact]
    public async Task PlainHttpTransport_WaitsForRetryAfterBeforeTheNextRequest()
    {
        var handler = new ThrottlingHandler(TimeSpan.FromSeconds(1));
        var settings = new OpAmpClientSettings
        {
            ServerUrl = new Uri("http://localhost/v1/opamp"),
            HttpClientFactory = () => new HttpClient(handler),
        };
        using var transport = new PlainHttpTransport(settings, new FrameProcessor());

        await Assert.ThrowsAsync<HttpRequestException>(
            () => transport.SendAsync(FrameGenerator.GenerateMockAgentFrame().Frame, CancellationToken.None));
        var throttledAt = Stopwatch.GetTimestamp();

        await transport.SendAsync(FrameGenerator.GenerateMockAgentFrame().Frame, CancellationToken.None);

        var waited = TimeSpan.FromSeconds((double)(handler.SecondRequestAt - throttledAt) / Stopwatch.Frequency);
        Assert.True(waited >= TimeSpan.FromSeconds(0.9), $"second request after {waited}, Retry-After was 1 s");
    }

    // First request: 429 with Retry-After. Later ones: an empty ServerToAgent.
    private sealed class ThrottlingHandler(TimeSpan retryAfter) : HttpMessageHandler
    {
        private int requests;

        public long SecondRequestAt { get; private set; }

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            if (Interlocked.Increment(ref this.requests) == 1)
            {
                var throttled = new HttpResponseMessage((HttpStatusCode)429);
                throttled.Headers.RetryAfter = new RetryConditionHeaderValue(retryAfter);
                return Task.FromResult(throttled);
            }

            this.SecondRequestAt = Stopwatch.GetTimestamp();
            return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK) { Content = new ByteArrayContent([]) });
        }
    }
}
