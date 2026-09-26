// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#if NETFRAMEWORK
using System.Net.Http;
#endif

using System.Net;
using Google.Protobuf;
using OpAmp.Proto.V1;
using OpenTelemetry.Internal;
using OpenTelemetry.OpAmp.Client.Internal.Utils;
using OpenTelemetry.OpAmp.Client.Settings;

namespace OpenTelemetry.OpAmp.Client.Internal.Transport.Http;

internal sealed class PlainHttpTransport : IOpAmpTransport, IDisposable
{
    private const string HeaderContentType = "Content-Type";
    private const string HeaderOpAmpInstanceUUID = "OpAMP-Instance-UID";

    private readonly Uri uri;
    private readonly HttpClient httpClient;
    private readonly FrameProcessor processor;

    // Retry-After from the last 429 / 503: no request before this (Stopwatch ticks; 0: none).
    private long notBefore;

    public PlainHttpTransport(OpAmpClientSettings settings, FrameProcessor processor)
    {
        Guard.ThrowIfNull(settings, nameof(settings));
        Guard.ThrowIfNull(processor, nameof(processor));

        this.uri = settings.ServerUrl;
        this.processor = processor;
        this.httpClient = settings.HttpClientFactory();
    }

    public bool RequiresResponseBeforeNextSend => true;

    public async Task SendAsync<T>(T message, CancellationToken token)
        where T : IMessage<T>
    {
        var content = message.ToByteArray();

        using var byteContent = new ByteArrayContent(content);
        byteContent.Headers.Add(HeaderContentType, "application/x-protobuf");

        if (message is AgentToServer { InstanceUid.Length: 16 } agentToServer)
        {
            var instanceUid = GuidExtensions.FromBigEndianBytes(agentToServer.InstanceUid.Span);
            byteContent.Headers.Add(HeaderOpAmpInstanceUUID, instanceUid.ToString());
        }

        using var request = new HttpRequestMessage(HttpMethod.Post, this.uri)
        {
            Content = byteContent,
        };

        await RetryAfter.WaitAsync(Interlocked.Read(ref this.notBefore), token).ConfigureAwait(false);

        // ResponseHeadersRead prevents HttpClient from buffering the entire response body
        // before we can enforce the transport size limit.
        using var response = await this.httpClient
            .SendAsync(request, HttpCompletionOption.ResponseHeadersRead, token)
            .ConfigureAwait(false);

        if (response.StatusCode is (HttpStatusCode)429 or HttpStatusCode.ServiceUnavailable
            && RetryAfter.FromHeader(response.Headers.RetryAfter, DateTimeOffset.UtcNow) is { } wait)
        {
            Interlocked.Exchange(ref this.notBefore, RetryAfter.Deadline(wait));
        }

        response.EnsureSuccessStatusCode();

        var responseMessage = await HttpClientHelpers.GetResponseBodyAsByteArrayAsync(
            TransportConstants.MaxMessageSize,
            response,
            token).ConfigureAwait(false);

        OpAmpClientEventSource.Log.HttpResponseBytesReceived(responseMessage.Length);

        this.processor.OnServerFrame(responseMessage.AsSequence());
    }

    public void Dispose() => this.httpClient?.Dispose();
}
