// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

using Google.Protobuf;
using OpenTelemetry.Internal;
using OpenTelemetry.OpAmp.Client.Settings;

namespace OpenTelemetry.OpAmp.Client.Internal.Transport.WebSocket;

/// <summary>
/// A WebSocket transport that reconnects when the connection is lost.
/// </summary>
/// <remarks>
/// A <see cref="WsTransport"/> is single-use. This keeps one open and, when its connection ends
/// without <see cref="StopAsync"/> (the server closed it, or the network dropped it), opens a new
/// one, retrying with exponential backoff and jitter. <see cref="Reconnected"/> fires after each
/// reconnect so the pipe can identify itself on the new connection.
/// </remarks>
internal sealed class ReconnectingWsTransport : IOpAmpTransport, IDisposable
{
    private readonly Func<WsTransport> factory;
    private readonly TimeSpan initialBackoff;
    private readonly TimeSpan maxBackoff;
    private readonly CancellationTokenSource stopping = new();
    private readonly Lock sync = new();

    private WsTransport? current;
    private Task? supervisor;
    private bool disposed;

    public ReconnectingWsTransport(OpAmpClientSettings settings, FrameProcessor processor)
        : this(() => new WsTransport(settings, processor), TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(30))
    {
        Guard.ThrowIfNull(settings, nameof(settings));
        Guard.ThrowIfNull(processor, nameof(processor));
    }

    internal ReconnectingWsTransport(Func<WsTransport> factory, TimeSpan initialBackoff, TimeSpan maxBackoff)
    {
        this.factory = factory;
        this.initialBackoff = initialBackoff;
        this.maxBackoff = maxBackoff;
    }

    /// <summary>
    /// Raised after a lost connection has been replaced by a new one.
    /// </summary>
    public event Action? Reconnected;

    public bool RequiresResponseBeforeNextSend => false;

    /// <summary>
    /// Opens the first connection. Throws if it can't be opened, as <see cref="WsTransport"/> does;
    /// only a connection lost later is retried.
    /// </summary>
    /// <param name="token">A cancellation token for the first connect.</param>
    /// <returns>A task that completes once the first connection is open.</returns>
    public async Task StartAsync(CancellationToken token = default)
    {
        var first = this.factory();
        try
        {
            await first.StartAsync(token).ConfigureAwait(false);
        }
        catch
        {
            first.Dispose();
            throw;
        }

        lock (this.sync)
        {
            this.current = first;
        }

        // Dispose waits for the supervisor before disposing anything it uses.
#pragma warning disable CA2025
        this.supervisor = Task.Run(() => this.SuperviseAsync(first), CancellationToken.None);
#pragma warning restore CA2025
    }

    public async Task StopAsync(CancellationToken token = default)
    {
#if NET8_0_OR_GREATER
        await this.stopping.CancelAsync().ConfigureAwait(false);
#else
        this.stopping.Cancel();
#endif

        WsTransport? transport;
        lock (this.sync)
        {
            transport = this.current;
        }

        if (transport != null)
        {
            await transport.StopAsync(token).ConfigureAwait(false);
        }
    }

    public Task SendAsync<T>(T message, CancellationToken token = default)
        where T : IMessage<T>
    {
        WsTransport? transport;
        lock (this.sync)
        {
            transport = this.current;
        }

        return transport == null
            ? Task.FromException(new InvalidOperationException("The WebSocket is not connected."))
            : transport.SendAsync(message, token);
    }

    public void Dispose()
    {
        if (this.disposed)
        {
            return;
        }

        this.disposed = true;
        this.stopping.Cancel();

        WsTransport? transport;
        lock (this.sync)
        {
            transport = this.current;
            this.current = null;
        }

        transport?.Dispose();

        try
        {
            this.supervisor?.GetAwaiter().GetResult();
        }
        catch (Exception ex)
        {
            OpAmpClientEventSource.Log.TransportCloseException(ex);
        }

        this.stopping.Dispose();
    }

    private async Task SuperviseAsync(WsTransport transport)
    {
        var token = this.stopping.Token;
        var random = new Random();
        while (!token.IsCancellationRequested)
        {
            // Wait for the connection to end: a Close frame, a network error, or our own stop.
            try
            {
                await transport.Completion.ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                OpAmpClientEventSource.Log.TransportCloseException(ex);
            }

            if (token.IsCancellationRequested)
            {
                return;
            }

            lock (this.sync)
            {
                this.current = null;
            }

            transport.Dispose();

            // Reconnect with exponential backoff and jitter (spec: Establishing Connection).
            var attempt = 0;
            while (true)
            {
                var ceiling = Math.Min(this.maxBackoff.TotalMilliseconds, this.initialBackoff.TotalMilliseconds * Math.Pow(2, attempt));

                // Jitter only spreads reconnects out; it needs no secure randomness.
#pragma warning disable CA5394
                var delay = TimeSpan.FromMilliseconds((ceiling / 2) + (random.NextDouble() * ceiling / 2));
#pragma warning restore CA5394
                try
                {
                    await Task.Delay(delay, token).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    return;
                }

                var next = this.factory();
                try
                {
                    await next.StartAsync(token).ConfigureAwait(false);
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    OpAmpClientEventSource.Log.TransportCloseException(ex);
                    next.Dispose();
                    attempt = Math.Min(attempt + 1, 30);
                    continue;
                }
                catch (OperationCanceledException)
                {
                    next.Dispose();
                    return;
                }

                lock (this.sync)
                {
                    this.current = next;
                }

                transport = next;
                break;
            }

            this.Reconnected?.Invoke();
        }
    }
}
