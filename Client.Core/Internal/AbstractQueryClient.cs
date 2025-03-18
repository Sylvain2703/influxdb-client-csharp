using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using InfluxDB.Client.Core.Exceptions;
using InfluxDB.Client.Core.Flux.Domain;
using InfluxDB.Client.Core.Flux.Internal;
using Newtonsoft.Json.Linq;
using RestSharp;
using RestSharp.Interceptors;

namespace InfluxDB.Client.Core.Internal
{
    public abstract class AbstractQueryClient : AbstractRestClient
    {
        protected static readonly Action EmptyAction = () => { };

        protected static readonly Action<Exception> ErrorConsumer = e => throw e;

        private readonly FluxCsvParser _csvParser;

        protected RestClient RestClient;
        protected readonly IFluxResultMapper Mapper;

        protected AbstractQueryClient(IFluxResultMapper mapper) : this(mapper, new FluxCsvParser())
        {
        }

        protected AbstractQueryClient(IFluxResultMapper mapper, FluxCsvParser csvParser)
        {
            Arguments.CheckNotNull(mapper, nameof(mapper));

            Mapper = mapper;
            _csvParser = csvParser;
        }

        protected Task Query(RestRequest query,
            FluxCsvParser.IFluxResponseConsumer responseConsumer,
            Action<Exception> onError,
            Action onComplete, CancellationToken cancellationToken)
        {
            void Consumer(Stream bufferedStream)
            {
                try
                {
                    _csvParser.ParseFluxResponse(bufferedStream, cancellationToken, responseConsumer);
                }
                catch (IOException e)
                {
                    onError(e);
                }
            }

            return Query(query, Consumer, onError, onComplete, cancellationToken);
        }

        protected Task QueryRaw(RestRequest query,
            Action<string> onResponse,
            Action<Exception> onError,
            Action onComplete, CancellationToken cancellationToken)
        {
            void Consumer(Stream bufferedStream)
            {
                try
                {
                    using var sr = new StreamReader(bufferedStream);

                    while (sr.ReadLine() is string line && !cancellationToken.IsCancellationRequested)
                        onResponse(line);
                }
                catch (IOException e)
                {
                    CatchOrPropagateException(e, onError);
                }
            }

            return Query(query, Consumer, onError, onComplete, cancellationToken);
        }

        protected void QuerySync(RestRequest query,
            FluxCsvParser.IFluxResponseConsumer responseConsumer,
            Action<Exception> onError,
            Action onComplete,
            CancellationToken cancellationToken)
        {
            void Consumer(Stream bufferedStream)
            {
                try
                {
                    _csvParser.ParseFluxResponse(bufferedStream, cancellationToken, responseConsumer);
                }
                catch (IOException e)
                {
                    onError(e);
                }
            }

            QuerySync(query, Consumer, onError, onComplete, cancellationToken);
        }

        private async Task Query(RestRequest query,
            Action<Stream> consumer,
            Action<Exception> onError, Action onComplete, CancellationToken cancellationToken)
        {
            Arguments.CheckNotNull(query, nameof(query));
            Arguments.CheckNotNull(consumer, nameof(consumer));
            Arguments.CheckNotNull(onError, nameof(onError));
            Arguments.CheckNotNull(onComplete, nameof(onComplete));

            try
            {
                query.Interceptors = new List<Interceptor> { new BeforeAfterRequestInterceptor(this, consumer) };
                query.AdvancedResponseWriter = CreateRestResponse;

                var restResponse = await RestClient.ExecuteAsync(query, cancellationToken).ConfigureAwait(false);
                if (restResponse.ErrorException != null)
                {
                    throw restResponse.ErrorException;
                }

                if (!cancellationToken.IsCancellationRequested)
                {
                    onComplete();
                }
            }
            catch (Exception e)
            {
                onError(e);
            }
        }

        private void QuerySync(RestRequest query,
            Action<Stream> consumer,
            Action<Exception> onError, Action onComplete, CancellationToken cancellationToken)
        {
            Arguments.CheckNotNull(query, nameof(query));
            Arguments.CheckNotNull(consumer, nameof(consumer));
            Arguments.CheckNotNull(onError, nameof(onError));
            Arguments.CheckNotNull(onComplete, nameof(onComplete));

            try
            {
                query.Interceptors = new List<Interceptor> { new BeforeAfterRequestInterceptor(this, consumer) };
                query.AdvancedResponseWriter = CreateRestResponse;

                var restResponse = RestClient.ExecuteSync(query, cancellationToken);
                if (restResponse.ErrorException != null)
                {
                    throw restResponse.ErrorException;
                }

                if (!cancellationToken.IsCancellationRequested)
                {
                    onComplete();
                }
            }
            catch (Exception e)
            {
                onError(e);
            }
        }

        protected async IAsyncEnumerable<T> QueryEnumerable<T>(
            RestRequest query,
            Func<FluxRecord, T> convert,
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            Arguments.CheckNotNull(query, nameof(query));

            query.Interceptors = new List<Interceptor> { new BeforeAfterRequestInterceptor(this, null) };

            using var stream = await RestClient.DownloadStreamAsync(query, cancellationToken).ConfigureAwait(false);
            using var sr = new StreamReader(stream);

            await foreach (var (_, record) in _csvParser
                               .ParseFluxResponseAsync(sr, cancellationToken).ConfigureAwait(false))
                if (!(record is null))
                {
                    yield return convert.Invoke(record);
                }
        }

        protected abstract void BeforeIntercept(RestRequest query);

        protected abstract T AfterIntercept<T>(int statusCode, Func<IEnumerable<HeaderParameter>> headers, T body);


        /// <summary>
        /// A RestSharp interceptor for <see cref="AbstractQueryClient"/> to modify requests and handle responses.
        /// </summary>
        private class BeforeAfterRequestInterceptor : Interceptor
        {
            private readonly AbstractQueryClient _queryClient;
            private readonly Action<Stream> _resultStreamConsumer;

            /// <summary>
            /// Construct the <see cref="AbstractQueryClient"/> request interceptor.
            /// </summary>
            /// <param name="queryClient">Client to which intercepted events are forwarded.</param>
            /// <param name="resultStreamConsumer">Action to consume the raw result stream.</param>
            internal BeforeAfterRequestInterceptor(AbstractQueryClient queryClient, Action<Stream> resultStreamConsumer)
            {
                _queryClient = queryClient;
                _resultStreamConsumer = resultStreamConsumer;
            }

            public override ValueTask BeforeRequest(
                RestRequest request, CancellationToken cancellationToken)
            {
                _queryClient.BeforeIntercept(request);
                return new ValueTask();
            }

            public override async ValueTask AfterHttpRequest(
                HttpResponseMessage response, CancellationToken cancellationToken)
            {
                var stream = await GetStreamFromResponseAsync(response, cancellationToken);
                stream = _queryClient.AfterIntercept(
                    (int)response.StatusCode,
                    () => response.Headers.ToHeaderParameters(response.Content.Headers),
                    stream);

                ThrowOnInfluxError(response, stream);
                _resultStreamConsumer?.Invoke(stream);
            }
        }

        public class FluxResponseConsumerPoco : FluxCsvParser.IFluxResponseConsumer
        {
            private readonly Action<object> _onNext;
            private readonly IFluxResultMapper _converter;
            private readonly Type _type;

            public FluxResponseConsumerPoco(Action<object> onNext, IFluxResultMapper converter, Type type)
            {
                _onNext = onNext;
                _converter = converter;
                _type = type;
            }

            public void Accept(int index, FluxTable table)
            {
            }

            public void Accept(int index, FluxRecord record)
            {
                _onNext(_converter.ConvertToEntity(record, _type));
            }
        }

        public class FluxResponseConsumerPoco<T> : FluxCsvParser.IFluxResponseConsumer
        {
            private readonly Action<T> _onNext;
            private readonly IFluxResultMapper _converter;

            public FluxResponseConsumerPoco(Action<T> onNext, IFluxResultMapper converter)
            {
                _onNext = onNext;
                _converter = converter;
            }

            public void Accept(int index, FluxTable table)
            {
            }

            public void Accept(int index, FluxRecord record)
            {
                _onNext(_converter.ConvertToEntity<T>(record));
            }
        }

        public static string GetDefaultDialect()
        {
            var json = new JObject();
            json.Add("header", true);
            json.Add("delimiter", ",");
            json.Add("quoteChar", "\"");
            json.Add("commentPrefix", "#");
            json.Add("annotations", new JArray("datatype", "group", "default"));

            return json.ToString();
        }

        public static string CreateBody(string dialect, string query)
        {
            Arguments.CheckNonEmptyString(query, "Flux query");

            var json = new JObject();
            json.Add("query", query);

            if (!string.IsNullOrEmpty(dialect))
            {
                json.Add("dialect", JObject.Parse(dialect));
            }

            return json.ToString();
        }

        protected static void CatchOrPropagateException(Exception exception, Action<Exception> onError)
        {
            Arguments.CheckNotNull(exception, nameof(exception));
            Arguments.CheckNotNull(onError, nameof(onError));

            //
            // Socket closed by remote server or end of data
            //
            if (exception is EndOfStreamException)
            {
                Trace.WriteLine("Socket closed by remote server or end of data",
                    InfluxDBTraceFilter.CategoryInfluxQueryError);
                Trace.WriteLine(exception, InfluxDBTraceFilter.CategoryInfluxQueryError);
            }
            else
            {
                onError(exception);
            }
        }

        protected static void ThrowOnInfluxError(RestResponse restResponse, object body)
        {
            if (restResponse.IsSuccessful)
            {
                return;
            }

            if (restResponse.ErrorException is InfluxException)
            {
                throw restResponse.ErrorException;
            }

            throw HttpException.Create(restResponse, body);
        }

        protected static void ThrowOnInfluxError(HttpResponseMessage httpResponse, object body)
        {
            if (httpResponse.IsSuccessStatusCode)
            {
                return;
            }

            throw HttpException.Create(httpResponse, body);
        }

        protected class FluxResponseConsumerRecord : FluxCsvParser.IFluxResponseConsumer
        {
            private readonly Action<FluxRecord> _onNext;

            public FluxResponseConsumerRecord(Action<FluxRecord> onNext)
            {
                _onNext = onNext;
            }

            public void Accept(int index, FluxTable table)
            {
            }

            public void Accept(int index, FluxRecord record)
            {
                _onNext(record);
            }
        }

        private static RestResponse CreateRestResponse(HttpResponseMessage response, RestRequest request)
        {
            return new RestResponse(request)
            {
                ErrorException = response.IsSuccessStatusCode
                    ? null
                    : new HttpRequestException($"Request failed with status code {response.StatusCode}")
            };
        }

        private static async Task<Stream> GetStreamFromResponseAsync(
            HttpResponseMessage response, CancellationToken cancellationToken)
        {
#if NET5_0_OR_GREATER
            var stream = await response.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
#else
            var stream = await response.Content.ReadAsStreamAsync().ConfigureAwait(false);
#endif

            if (response.Content.Headers.ContentEncoding.Any(x => "gzip".Equals(x, StringComparison.OrdinalIgnoreCase)))
            {
                stream = new GZipStream(stream, CompressionMode.Decompress);
            }

            return stream;
        }
    }
}