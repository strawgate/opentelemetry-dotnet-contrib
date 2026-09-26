// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

using System.Diagnostics;
using System.Net.Http.Headers;

namespace OpenTelemetry.OpAmp.Client.Internal.Utils;

/// <summary>
/// How long the server asked the client to wait (Retry-After, retry_info), as a monotonic
/// deadline. A server can't park a client for more than an hour.
/// </summary>
internal static class RetryAfter
{
    internal static readonly TimeSpan Max = TimeSpan.FromHours(1);

    /// <summary>
    /// The wait a Retry-After header asks for: delta-seconds, or an HTTP date relative to now.
    /// </summary>
    /// <param name="header">The response's Retry-After header, if any.</param>
    /// <param name="now">The current time.</param>
    /// <returns>The wait, or <see langword="null"/> without a header.</returns>
    public static TimeSpan? FromHeader(RetryConditionHeaderValue? header, DateTimeOffset now)
    {
        if (header?.Delta is { } delta)
        {
            return Clamp(delta);
        }

        return header?.Date is { } date ? Clamp(date - now) : null;
    }

    /// <summary>
    /// The wait a ServerErrorResponse's retry_info asks for.
    /// </summary>
    /// <param name="retryAfterNanoseconds">retry_info.retry_after_nanoseconds.</param>
    /// <returns>The wait.</returns>
    public static TimeSpan FromRetryInfo(ulong retryAfterNanoseconds)
        => Clamp(TimeSpan.FromTicks((long)Math.Min(retryAfterNanoseconds / 100, (ulong)Max.Ticks)));

    /// <summary>
    /// A deadline <paramref name="wait"/> from now, in <see cref="Stopwatch"/> ticks.
    /// </summary>
    /// <param name="wait">How long to wait.</param>
    /// <returns>The deadline.</returns>
    public static long Deadline(TimeSpan wait)
        => Stopwatch.GetTimestamp() + (long)(wait.TotalSeconds * Stopwatch.Frequency);

    /// <summary>
    /// Waits until <paramref name="deadline"/> (0: no deadline).
    /// </summary>
    /// <param name="deadline">A deadline from <see cref="Deadline"/>.</param>
    /// <param name="token">Cancellation token.</param>
    /// <returns>A task that completes at the deadline.</returns>
    public static Task WaitAsync(long deadline, CancellationToken token)
    {
        var remaining = deadline - Stopwatch.GetTimestamp();
        return deadline == 0 || remaining <= 0
            ? Task.CompletedTask
            : Task.Delay(TimeSpan.FromSeconds((double)remaining / Stopwatch.Frequency), token);
    }

    private static TimeSpan Clamp(TimeSpan wait)
        => wait < TimeSpan.Zero ? TimeSpan.Zero : wait > Max ? Max : wait;
}
