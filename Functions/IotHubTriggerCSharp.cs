using IoTHubTrigger = Microsoft.Azure.WebJobs.EventHubTriggerAttribute;

using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Azure.EventHubs;
using System.Text;
using System.Net.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using System;
using System.Web;
using System.IO;
using Microsoft.Azure.Devices.Client;
using System.Text.RegularExpressions;

namespace Company.Function
{
    public class PredictionMessage
    {
        public string Type { get; set; }
        public DateTime PredictionDate { get; set; }

        public float LandingLat { get; set; }
        public float LandingLong { get; set; }
        public DateTime LandingDateTime { get; set; }

    }
    public class HabHubPredictionResponse
    {
        public string valid { get; set; }
        public string uuid { get; set; }
        public int timestamp { get; set; }
    }
    public class HabHubPredictionPoint
    {
        public Int64 Timestamp { get; set; }
        public float Latitude { get; set; }
        public float Longitude { get; set; }
        public float Altitude { get; set; }

    }
    public class IoTMessage
    {

        public double lat { get; set; }
        public double lon { get; set; }
        public double initial_alt { get; set; }
        public double hour { get; set; }
        public double min { get; set; }
        public double second { get; set; }
        public double day { get; set; }
        public double month { get; set; }
        public double year { get; set; }
        public double ascent { get; set; }
        public double drag { get; set; }
        public double burst { get; set; }

    }




    public static class IotHubTriggerCSharp
    {
        // for time conversion
        private static readonly DateTime epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        private static DeviceClient s_deviceClient;
        private readonly static string s_connectionString = "HostName=TestHubBH.azure-devices.net;DeviceId=TestDevice;SharedAccessKey=fHQknoNBTkVoG1auCpl8Ma9l5QRf8Hxy/flmlIrIEfk=";
        private static DateTime UnixTimeToDateTime(long unixTime)
        {
            return epoch.AddSeconds(unixTime);
        }
        private static HttpClient client = new HttpClient();

        [FunctionName("IotHubTriggerCSharp")]
        public static void Run([IoTHubTrigger("messages/events", Connection = "IotConn")]EventData message, ILogger log)
        {
            var receivedIoT = JsonConvert.DeserializeObject<IoTMessage>(Encoding.UTF8.GetString(message.Body.Array));
            var postValues = new Dictionary<string, string>
            {
                //{ "action","submit"},
                { "lat",  receivedIoT.lat.ToString()},
                { "lon",  receivedIoT.lon.ToString()},
                { "initial_alt", receivedIoT.initial_alt.ToString()},
                { "hour", receivedIoT.hour.ToString()},
                { "min", receivedIoT.min.ToString()},
                { "second", receivedIoT.second.ToString()},
                { "day", receivedIoT.day.ToString()},
                { "month", receivedIoT.month.ToString()},
                { "year", receivedIoT.year.ToString()},
                { "ascent", receivedIoT.ascent.ToString()},
                { "drag", receivedIoT.drag.ToString()},
                { "burst", receivedIoT.burst.ToString()},
                { "submit", "Run Prediction"}
            };

            var content = new FormUrlEncodedContent(postValues);
            postFunction(content);
            //log.LogInformation($"C# IoT Hub trigger function processed a message: {Encoding.UTF8.GetString(message.Body.Array)}");
        }

        private static async Task postFunction(FormUrlEncodedContent content)
        {
            var response = await client.PostAsync("http://predict.habhub.org/ajax.php?action=submitForm", content);

            if (response.IsSuccessStatusCode)
            {
                var url = buildUrl(response).Result;
                response = await client.GetAsync(url);

                if (response.IsSuccessStatusCode)
                {
                    var csv = getSanitizedCsv(response).Result;                    
                    var records = getRecords(csv);

                    if (records.Count >= 2)
                    {
                        var predictionMessage = generatePredictionMessage(records);
                        sendPrediction(predictionMessage);
                    }
                }
            }
        }


        private static async void sendPrediction(PredictionMessage prediction)
        {
            var contentType = "Prediction";

            var messageString = JsonConvert.SerializeObject(prediction);
            var message = new Message(Encoding.ASCII.GetBytes(messageString));
            message.ContentType = contentType;
            s_deviceClient = DeviceClient.CreateFromConnectionString(s_connectionString, Microsoft.Azure.Devices.Client.TransportType.Mqtt);
            await s_deviceClient.SendEventAsync(message);
            Console.WriteLine(message.ContentType.ToString());
            Console.WriteLine("{0} > Sending message: {1}", DateTime.Now, messageString);
            await Task.Delay(1000);

        }

        private static async Task<string> buildUrl(HttpResponseMessage response)
        {
            var responseString = await response.Content.ReadAsStringAsync();

            var predictionResponse = JsonConvert.DeserializeObject<HabHubPredictionResponse>(responseString);

            // example request: http://predict.habhub.org/ajax.php?action=getCSV&uuid=202f57505788f22612beb5093b66ff53c60a43b5

            var builder = new UriBuilder("http://predict.habhub.org/ajax.php");
            builder.Port = -1;
            var query = HttpUtility.ParseQueryString(builder.Query);
            query["action"] = "getCSV";
            query["uuid"] = predictionResponse.uuid;
            builder.Query = query.ToString();
            var url = builder.ToString();

            return url;

        }

        private static async Task<string> getSanitizedCsv(HttpResponseMessage response)
        {
            var responseString = await response.Content.ReadAsStringAsync();
            var csvString = Regex.Replace(responseString, "\\\",\\\"", ",\r\n").Replace("[", "").Replace("]", "");
            var csvString2 = Regex.Replace(csvString, "\\\"", "");

            return csvString2;

        }

        private static List<HabHubPredictionPoint> getRecords(string csv)
        {
            var csvOfRecords = new CsvHelper.CsvReader(new StringReader(csv));

            csvOfRecords.Configuration.IgnoreQuotes = true;
            csvOfRecords.Configuration.HasHeaderRecord = false;
            csvOfRecords.Configuration.MissingFieldFound = null;
            csvOfRecords.Read();
            var records = new List<HabHubPredictionPoint>(csvOfRecords.GetRecords<HabHubPredictionPoint>());

            return records;
        }

        private static PredictionMessage generatePredictionMessage(List<HabHubPredictionPoint> records)
        {
            var landingRecord = records[records.Count - 2];
            var predictionMessage = new PredictionMessage();
            predictionMessage.PredictionDate = DateTime.UtcNow;
            predictionMessage.LandingDateTime = UnixTimeToDateTime(landingRecord.Timestamp);
            predictionMessage.LandingLat = landingRecord.Latitude;
            predictionMessage.LandingLong = landingRecord.Longitude;

            return predictionMessage;
        }
    }
}