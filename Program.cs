using System;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Text.Json;
using System.Dynamic;
using OpenIAP;
class Program
{
    const string defaultWiq = "";
    private static void ProcessWorkitem(Workitem workitem)
    {
        Console.WriteLine($"Processing workitem id {workitem.id} retry #{workitem.retries}");
        if (workitem.payload == null) workitem.payload = "{}";
        workitem.name = "Hello kitty";
        var payload = JsonSerializer.Deserialize<ExpandoObject>(workitem.payload);
        if (payload == null)
        {
            throw new InvalidOperationException("Payload deserialization resulted in null.");
        }
#pragma warning disable CS8619 // Nullability of reference types in value doesn't match target type.
        var payloadDict = (IDictionary<string, object>)payload;
#pragma warning restore CS8619 // Nullability of reference types in value doesn't match target type.
        payloadDict["name"] = "Hello kitty";
        workitem.payload = JsonSerializer.Serialize(payload);
    }

    static readonly List<string> preserveFiles = new List<string>();
    private static async Task ProcessWorkitemWrapper(Client client, Workitem workitem)
    {
        var originalFiles = Directory.GetFiles(Directory.GetCurrentDirectory())
                                    .Where(f => File.Exists(f))
                                    .ToList();
        try
        {
            var preserveFiles = Directory.GetFiles(Directory.GetCurrentDirectory())
                                       .Where(f => File.Exists(f))
                                       .ToList();

            ProcessWorkitem(workitem);
            workitem.state = "successful";
        }
        catch (Exception error)
        {
            workitem.state = "retry";
            workitem.errortype = "application"; // business rule will never retry / application will retry as many times as defined on the workitem queue
            workitem.errormessage = error.Message;
            workitem.errorsource = error.StackTrace ?? "";
        }

        var currentFiles = Directory.GetFiles(Directory.GetCurrentDirectory())
                                   .Where(f => File.Exists(f))
                                   .ToList();
        var newFiles = currentFiles.Except(preserveFiles).ToArray();

        await client.UpdateWorkitem(workitem, newFiles);

        // Cleanup new files
        var filesToDelete = Directory.GetFiles(Directory.GetCurrentDirectory())
                                    .Where(f => File.Exists(f) && !originalFiles.Contains(f));
        foreach (var file in filesToDelete)
        {
            File.Delete(file);
        }
    }
    private static void SetupQueueListening(Client client, Workitem workitem)
    {
        if (client.connected())
        {
            Console.WriteLine("Client connection success: " + client.connected());
            
            string wiq = Environment.GetEnvironmentVariable("wiq") ?? defaultWiq;
            string queue = Environment.GetEnvironmentVariable("queue") ?? wiq;
            string queuename = client.RegisterQueue(queue, async (queueEvent) => {
                try
                {
                    int counter = 0;
                    Workitem? workitem;
                    do
                    {
                        workitem = await client.PopWorkitem(wiq);
                        if (workitem != null)
                        {
                            counter++;
                            await ProcessWorkitemWrapper(client, workitem);
                        }
                    } while (workitem != null);

                    if (counter > 0)
                    {
                        Console.WriteLine($"No more workitems in {wiq} workitem queue");
                    }
                }
                catch (Exception error)
                {
                    Console.WriteLine($"Error processing queue: {error}");
                }
            });
            Console.WriteLine($"Consuming queue {queuename}");
        }

    }


    static async Task Main(string[] args)
    {
        // Initialize the client
        Console.WriteLine($"Creating client, Thread ID: {Thread.CurrentThread.ManagedThreadId}");
        Client client = new Client();
        client.enabletracing("openiap=info", "");

        await client.connect();
        client.on_client_event((e) =>
        {
            Console.WriteLine($"Client event: {e.evt} {e.reason}");
            if(e.evt == "SignedIn")
            {
                SetupQueueListening(client, new Workitem());
            }
        });

        if (!client.connected())
        {
            Console.WriteLine("Client connection error: " + client.connectionerror());
            return;
        }

        Console.ReadLine();

    }
}
