using System.IO.Pipes;
using System.Diagnostics;
using System.Net.WebSockets;
using Openiap;

// Force update
// dotnet nuget locals all -c
// dotnet remove package openiap.dotnetapi && dotnet add package openiap.dotnetapi --version 0.0.7

dynamic ProcessWorkitem(Workitem wi, dynamic payload)
{
    payload.name = "updated";
    if(payload.error != null) throw new Exception("Hi mom!!!");
    return payload;
}

var apiurl = Environment.GetEnvironmentVariable("apiurl");
if(apiurl == null || apiurl == "" ) apiurl = Environment.GetEnvironmentVariable("grpcapiurl");
var wiq = Environment.GetEnvironmentVariable("wiq");
var queue = Environment.GetEnvironmentVariable("queue");
if(queue == null || queue == "") queue = wiq;
if(wiq == null || queue == null || apiurl == null) {
    Console.WriteLine("wiq, apiurl and queue environment variables must be set");
    return;
}
openiap client = new openiap(apiurl);
client.OnSignedin = async (user) =>
{
    Console.WriteLine("Signed in as " + user.Username);
    try
    {
        var result = await client.RegisterQueue(queue, async (Openiap.QueueEvent qe, dynamic p) =>
        {
            var wi = await client.PopWorkitem(wiq);
            while (wi != null)
            {
                Console.WriteLine("Got workitem " + wi.Id);
                var json = wi.Payload;
                if(json == null || json == "") json = "{}";
                var payload = Newtonsoft.Json.JsonConvert.DeserializeObject<dynamic>(json);
                try
                {
                    payload = ProcessWorkitem(wi, payload);
                    if(payload != null) {
                        wi.Payload = Newtonsoft.Json.JsonConvert.SerializeObject(payload);
                    }
                    wi.State = "successful";
                    // client.UpdateWorkitem(wi);
                }
                catch (System.Exception ex)
                {
                    wi.State = "retry";
                    wi.Errormessage = ex.Message;
                    wi.Errorsource = ex.StackTrace;
                }
                client.UpdateWorkitem(wi, payload);
                wi = await client.PopWorkitem(wiq);
            }
            return null;
        });
    }
    catch (System.Exception ex)
    {
        Console.WriteLine("Failed to create watch " + ex.Message);
    }
    // return Task.CompletedTask;
};
await protowrap.Connect(client);
do
{
    await Task.Delay(500);
} while (true);