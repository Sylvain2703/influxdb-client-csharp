using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using InfluxDB.Client.Api.Domain;
using InfluxDB.Client.Core;
using NUnit.Framework;

namespace InfluxDB.Client.Test
{
    public class HistoryBarConstant
    {
        public static readonly string Bucket = "my-bucket";
        public static readonly string OrgId = "my-org";
    }

    [Measurement("history_bar_3")]
    public class HistoryBar
    {
        [Column("value")] public double? Value { get; set; }

        [Column(IsTimestamp = true)] public DateTime Date { get; set; }
    }

    [TestFixture]
    [Ignore("Only example")]
    public class ItWriteManyMeasurements
    {
        private static readonly int MaxBarsPerRequest = 50_000;
        private static readonly int CountToWrite = 2_000_000;
        private List<HistoryBar> bars = new List<HistoryBar>();

        [SetUp]
        public void SetUp()
        {
            for (var i = 0; i < CountToWrite; i++)
                bars.Add(new HistoryBar { Value = i, Date = DateTimeEx.UnixEpoch.Add(TimeSpan.FromSeconds(i)) });
        }

        [Test]
        public async Task Write()
        {
            var client = new InfluxDBClient("http://localhost:9999", "my-token");
            var api = client.GetWriteApi(new WriteOptions { BatchSize = MaxBarsPerRequest, FlushInterval = 10_000 });

            var start = 0;
            for (;;)
            {
                var historyBars = bars.Skip(start).Take(MaxBarsPerRequest).ToArray();
                if (historyBars.Length == 0)
                {
                    break;
                }

                if (start != 0)
                {
                    Trace.WriteLine("Delaying...");
                    await Task.Delay(100);
                }

                start += MaxBarsPerRequest;
                Trace.WriteLine(
                    $"Add bars to buffer From: {historyBars.First().Date}, To: {historyBars.Last().Date}. Remaining {CountToWrite - start}");
                api.WriteMeasurements(historyBars, WritePrecision.S, HistoryBarConstant.Bucket,
                    HistoryBarConstant.OrgId);
            }

            Trace.WriteLine("Flushing data...");

            client.Dispose();

            Trace.WriteLine("Finished");
        }
    }
}