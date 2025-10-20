using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Threading;


try
{
    var mongoConnection = "mongodb://172.19.0.1:27017/?replicaSet=rs0";
    var client = new MongoClient(mongoConnection);

    var db = client.GetDatabase("accountingdb");
    var coll = db.GetCollection<BsonDocument>("transactions");

    BsonDocument resumeToken = LoadResumeToken();

    var options = new ChangeStreamOptions
    {
        FullDocument = ChangeStreamFullDocumentOption.Required,
        FullDocumentBeforeChange = ChangeStreamFullDocumentBeforeChangeOption.Required,
        StartAfter = resumeToken
    };


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
        coll.InsertOne(new BsonDocument());

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

/////////////////////////////////////////////////


using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Microsoft.EntityFrameworkCore;

#region Models & Context
public interface IHasHash
{
    int Id { get; set; }
    string Hash { get; set; }
}

public class User : IHasHash
{
    public int Id { get; set; }
    public string Name { get; set; }
    public string Family { get; set; }
    public string PhoneNumber { get; set; }
    public bool ConfirmPhoneNumber { get; set; }
    public string Hash { get; set; }
}

public class Role : IHasHash
{
    public int Id { get; set; }
    public string Name { get; set; }
    public string Hash { get; set; }
}

public class UserRole : IHasHash
{
    public int Id { get; set; }
    public int UserId { get; set; }
    public int RoleId { get; set; }
    public string Hash { get; set; }
}

public class Permission : IHasHash
{
    public int Id { get; set; }
    public string Name { get; set; }
    public string Hash { get; set; }
}

public class MyDbContext : DbContext
{
    public DbSet<User> Users => Set<User>();
    public DbSet<Role> Roles => Set<Role>();
    public DbSet<UserRole> UserRoles => Set<UserRole>();
    public DbSet<Permission> Permissions => Set<Permission>();
}
#endregion

#region DTO for JSON files
public class FileData
{
    public int Count { get; set; }
    public List<int> Items { get; set; } = new();
}
#endregion

#region Core Service
public sealed class IntegrityVerifier
{
    private readonly MyDbContext _db;
    private readonly HttpClient _http = new();

    public IntegrityVerifier(MyDbContext db)
    {
        _db = db;
    }

    public async Task<List<string>> VerifyAllAsync()
    {
        var errors = new ConcurrentBag<string>();

        var tasks = new List<Task>
        {
            Task.Run(() => VerifyUsersAsync(errors)),
            Task.Run(() => VerifyRolesAsync(errors)),
            Task.Run(() => VerifyUserRolesAsync(errors)),
            Task.Run(() => VerifyPermissionsAsync(errors)),
            Task.Run(() => CallExternalApiAsync("https://api1.service.com/data", errors)),
            Task.Run(() => CallExternalApiAsync("https://api2.service.com/data", errors))
        };

        await Task.WhenAll(tasks);
        return errors.ToList();
    }

    #region Table verifications
    private async Task VerifyUsersAsync(ConcurrentBag<string> errors)
    {
        var users = await _db.Users.AsNoTracking()
            .Select(u => new
            {
                u.Id,
                u.Name,
                u.Family,
                u.PhoneNumber,
                u.ConfirmPhoneNumber,
                u.Hash
            }).ToListAsync();

        var file = await LoadFileAsync("user.json");
        CompareRecords("User", users.Select(u => (u.Id, RecomputeUserHash(u), u.Hash)), file, errors);
    }

    private async Task VerifyRolesAsync(ConcurrentBag<string> errors)
    {
        var roles = await _db.Roles.AsNoTracking()
            .Select(r => new { r.Id, r.Name, r.Hash }).ToListAsync();

        var file = await LoadFileAsync("role.json");
        CompareRecords("Role", roles.Select(r => (r.Id, ComputeHash($"{r.Id}|{r.Name}"), r.Hash)), file, errors);
    }

    private async Task VerifyUserRolesAsync(ConcurrentBag<string> errors)
    {
        var urs = await _db.UserRoles.AsNoTracking()
            .Select(ur => new { ur.Id, ur.UserId, ur.RoleId, ur.Hash }).ToListAsync();

        var file = await LoadFileAsync("userrole.json");
        CompareRecords("UserRole", urs.Select(ur => (ur.Id, ComputeHash($"{ur.UserId}|{ur.RoleId}"), ur.Hash)), file, errors);
    }

    private async Task VerifyPermissionsAsync(ConcurrentBag<string> errors)
    {
        var perms = await _db.Permissions.AsNoTracking()
            .Select(p => new { p.Id, p.Name, p.Hash }).ToListAsync();

        var file = await LoadFileAsync("permission.json");
        CompareRecords("Permission", perms.Select(p => (p.Id, ComputeHash($"{p.Id}|{p.Name}"), p.Hash)), file, errors);
    }
    #endregion

    #region Compare Logic
    private static void CompareRecords(string tableName, IEnumerable<(int Id, string NewHash, string OldHash)> data,
                                       FileData file, ConcurrentBag<string> errors)
    {
        var dbData = data.ToDictionary(x => x.Id, x => x.NewHash);

        // 1️⃣ تعداد رکوردها
        if (dbData.Count != file.Count)
            errors.Add($"{tableName}: Count mismatch DB={dbData.Count} File={file.Count}");

        // 2️⃣ بررسی وجود IDها
        var missingInDb = file.Items.Except(dbData.Keys).ToList();
        var missingInFile = dbData.Keys.Except(file.Items).ToList();

        if (missingInDb.Any())
            errors.Add($"{tableName}: Missing in DB => {string.Join(',', missingInDb)}");
        if (missingInFile.Any())
            errors.Add($"{tableName}: Missing in File => {string.Join(',', missingInFile)}");

        // 3️⃣ مقایسه هش‌ها
        foreach (var (id, newHash) in dbData)
        {
            var oldHash = data.First(x => x.Id == id).OldHash;
            if (!string.Equals(newHash, oldHash, StringComparison.Ordinal))
                errors.Add($"{tableName}: Hash mismatch Id={id}");
        }
    }
    #endregion

    #region Helpers
    private static async Task<FileData> LoadFileAsync(string filename)
    {
        var path = Path.Combine("data", filename);
        var json = await File.ReadAllTextAsync(path);
        return JsonSerializer.Deserialize<FileData>(json)!;
    }

    private static string RecomputeUserHash(dynamic u)
        => ComputeHash($"{u.Id}|{u.Name}|{u.Family}|{u.PhoneNumber}|{u.ConfirmPhoneNumber}");

    private static string ComputeHash(string raw)
    {
        using var sha = SHA256.Create();
        var bytes = sha.ComputeHash(Encoding.UTF8.GetBytes(raw));
        return Convert.ToHexString(bytes).ToLowerInvariant();
    }

    private async Task CallExternalApiAsync(string url, ConcurrentBag<string> errors)
    {
        try
        {
            var res = await _http.GetAsync(url);
            var content = await res.Content.ReadAsStringAsync();
            if (res.IsSuccessStatusCode)
            {
                errors.Add($"API {url} OK => {content}");
            }
            else
            {
                errors.Add($"API {url} FAILED => {res.StatusCode}");
            }
        }
        catch (Exception ex)
        {
            errors.Add($"API {url} Exception => {ex.Message}");
        }
    }
    #endregion
}
#endregion


var verifier = new IntegrityVerifier(dbContext);
var results = await verifier.VerifyAllAsync();

foreach (var msg in results)
    Console.WriteLine(msg);

