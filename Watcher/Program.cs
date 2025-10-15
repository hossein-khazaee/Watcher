using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.IO;
using System.Threading;


try
{
    var mongoConnection = "mongodb://172.19.0.1:27017/?replicaSet=rs0";
    var client = new MongoClient(mongoConnection);

    var db = client.GetDatabase("accountingdb");
    var coll = db.GetCollection<BsonDocument>("transactions");

    Console.WriteLine("✅ CDC Watcher started on single-node MongoDB (Primary)...");

    BsonDocument resumeToken = LoadResumeToken();

    var options = new ChangeStreamOptions
    {
        FullDocument = ChangeStreamFullDocumentOption.Required,
        FullDocumentBeforeChange = ChangeStreamFullDocumentBeforeChangeOption.Required,
        StartAfter = resumeToken
    };

    // --------------------------------------------
    // Begin Watching
    // --------------------------------------------
    using var cursor = coll.Watch(options);

    foreach (var change in cursor.ToEnumerable())
    {
        //Console.WriteLine("==== CHANGE DETECTED ====");
        //Console.WriteLine($"OperationType: {change.OperationType}");
        //Console.WriteLine($"Key: {change.DocumentKey}");
        //Console.WriteLine($"Before: {change.FullDocumentBeforeChange}");
        //Console.WriteLine($"After: {change.FullDocument}");
        //Console.WriteLine();

        SaveResumeToken(change.ResumeToken.AsBsonDocument);
    }

}
catch (Exception rx)
{
    var r = rx;
	throw;
}

static BsonDocument LoadResumeToken()
{
    if (File.Exists("resume_token.json"))
        return BsonDocument.Parse(File.ReadAllText("resume_token.json"));
    return null;
}

static void SaveResumeToken(BsonDocument token)
{
    File.WriteAllText("resume_token.json", token.ToJson());
}
