// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

using OpenTelemetry.OpAmp.Client.Internal;
using OpenTelemetry.OpAmp.Client.Internal.Transport.WebSocket;
using OpenTelemetry.OpAmp.Client.Settings;
using OpenTelemetry.OpAmp.Client.Tests.Tools;

namespace OpenTelemetry.OpAmp.Client.Tests;

public class ReconnectingWsTransportTests
{
    [Fact]
    public async Task ReconnectingWsTransport_ReconnectsAfterTheConnectionDrops()
    {
        var connections = 0;
        using var opAmpServer = new OpAmpFakeWebSocketServer(
            (frame, socket, token) =>
            {
                // Drop the first connection without a Close frame; keep the second.
                if (Interlocked.Increment(ref connections) == 1)
                {
                    socket.Abort();
                }

                return Task.CompletedTask;
            });

        using var transport = CreateTransport(opAmpServer.Endpoint);
        using var reconnected = new SemaphoreSlim(0);
        transport.Reconnected += () => reconnected.Release();

        await transport.StartAsync(CancellationToken.None);
        await transport.SendAsync(FrameGenerator.GenerateMockAgentFrame().Frame, CancellationToken.None);

        Assert.True(await reconnected.WaitAsync(TimeSpan.FromSeconds(10)), "no reconnect");

        // The new connection carries messages.
        await transport.SendAsync(FrameGenerator.GenerateMockAgentFrame().Frame, CancellationToken.None);
        Assert.True(SpinWait.SpinUntil(() => opAmpServer.GetFrames().Count == 2, TimeSpan.FromSeconds(10)));
        Assert.Equal(2, opAmpServer.GetRequestHeaders().Count);

        await transport.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task ReconnectingWsTransport_DoesNotReconnectAfterStop()
    {
        using var opAmpServer = new OpAmpFakeWebSocketServer(useSmallPackets: false);
        using var transport = CreateTransport(opAmpServer.Endpoint);
        var reconnects = 0;
        transport.Reconnected += () => Interlocked.Increment(ref reconnects);

        await transport.StartAsync(CancellationToken.None);
        await transport.StopAsync(CancellationToken.None);
        await Task.Delay(TimeSpan.FromMilliseconds(500));

        Assert.Equal(0, reconnects);
        Assert.Single(opAmpServer.GetRequestHeaders());
    }

    private static ReconnectingWsTransport CreateTransport(Uri endpoint)
    {
        var settings = new OpAmpClientSettings
        {
            ConnectionType = ConnectionType.WebSocket,
            ServerUrl = endpoint,
        };
        var processor = new FrameProcessor();

        return new ReconnectingWsTransport(
            () => new WsTransport(settings, processor),
            TimeSpan.FromMilliseconds(50),
            TimeSpan.FromMilliseconds(200));
    }
}
