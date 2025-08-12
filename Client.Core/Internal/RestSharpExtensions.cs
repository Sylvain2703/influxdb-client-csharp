using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http.Headers;
using System.Threading;
using RestSharp;
using RestSharp.Interceptors;

namespace InfluxDB.Client.Core.Internal
{
    internal static class RestSharpExtensions
    {
        /// <summary>
        /// Transform `HttpHeaders` to `HeaderParameter` type.
        /// </summary>
        /// <param name="httpHeaders"></param>
        /// <param name="httpContentHeaders">Additionally content Headers</param>
        /// <returns>IEnumerable&lt;HeaderParameter&gt;</returns>
        internal static IEnumerable<HeaderParameter> ToHeaderParameters(this HttpHeaders httpHeaders,
            HttpContentHeaders httpContentHeaders = null)
        {
            return httpHeaders
                .Concat(httpContentHeaders ?? Enumerable.Empty<KeyValuePair<string, IEnumerable<string>>>())
                .SelectMany(x => x.Value.Select(y => (x.Key, y)))
                .Select(x => new HeaderParameter(x.Key, x.y));
        }

        internal static bool HasHeader(this IEnumerable<HeaderParameter> headers, string name, string value)
        {
            return headers.Any(h =>
                name.Equals(h.Name, StringComparison.OrdinalIgnoreCase) &&
                value.Equals(h.Value, StringComparison.OrdinalIgnoreCase));
        }

        internal static RestResponse ExecuteSync(this RestClient client,
            RestRequest request, CancellationToken cancellationToken = default)
        {
            return client.Execute(request);
        }

        internal static RestClientOptions Copy(this ReadOnlyRestClientOptions options)
        {
            return new RestClientOptions
            {
                BaseUrl = options.BaseUrl,
                ConfigureMessageHandler = options.ConfigureMessageHandler,
                CalculateResponseStatus = options.CalculateResponseStatus,
                Authenticator = options.Authenticator,
                Credentials = options.Credentials,
                UseDefaultCredentials = options.UseDefaultCredentials,
                DisableCharset = options.DisableCharset,
                AutomaticDecompression = options.AutomaticDecompression,
                MaxRedirects = options.MaxRedirects,
                Proxy = options.Proxy,
                CachePolicy = options.CachePolicy,
                FollowRedirects = options.FollowRedirects,
                Expect100Continue = options.Expect100Continue,
                UserAgent = options.UserAgent,
                PreAuthenticate = options.PreAuthenticate,
                RemoteCertificateValidationCallback = options.RemoteCertificateValidationCallback,
                BaseHost = options.BaseHost,
                CookieContainer = options.CookieContainer,
                Timeout = options.Timeout,
                Encoding = options.Encoding,
                ThrowOnAnyError = options.ThrowOnAnyError,
                ThrowOnDeserializationError = options.ThrowOnDeserializationError,
                FailOnDeserializationError = options.FailOnDeserializationError,
                AllowMultipleDefaultParametersWithSameName = options.AllowMultipleDefaultParametersWithSameName,
                Encode = options.Encode,
                EncodeQuery = options.EncodeQuery,
                Interceptors = options.Interceptors?.ToList() ?? new List<Interceptor>()
            };
        }
    }
}