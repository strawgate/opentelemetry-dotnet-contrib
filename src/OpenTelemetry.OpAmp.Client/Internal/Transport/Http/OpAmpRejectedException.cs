// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#if NETFRAMEWORK
using System.Net.Http;
#endif

namespace OpenTelemetry.OpAmp.Client.Internal.Transport.Http;

/// <summary>
/// The server rejected a message (413 Content Too Large): unlike a refused or failed request, its
/// content must not simply be sent again.
/// </summary>
internal sealed class OpAmpRejectedException : HttpRequestException
{
    public OpAmpRejectedException()
    {
    }

    public OpAmpRejectedException(string message)
        : base(message)
    {
    }

    public OpAmpRejectedException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
