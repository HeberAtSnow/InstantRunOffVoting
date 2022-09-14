# InstantRunOffVoting

[schema](InstantRunOff_Schema.pdf)

[running the database](Instructions_How_To_Use_Instant_Run_Off_Calculations.pdf)

## Generating C# Classes to work with the database
<details>
  <summary>
    See a walkthrough of all the commands necesarry to build an API or web app to talk to the database.
  </summary>

First, make sure you have the EF Core CLI tools installed

```bash
dotnet tool install --global dotnet-ef
```

If you don't already have a project to work with, make one
> make an API

```bash
dotnet new webapi -n iro.api
```

> or make a web site

```bash
dotnet new webapp -n iro.web
```

You'll need to also make sure your project has the Postgres and Microsoft.EntityFrameworkCore.Design libraries
> Run that from the directory that has your code.  You may need to `cd iro.web` or something like that to get into the correct directory.

```bash
dotnet add package microsoft.entityframeworkcore.design
dotnet add package npgsql.entityframeworkcore.postgresql
```

Now you should be able to scaffold the database

```bash
dotnet ef dbcontext scaffold "host=localhost; database=iro; user id=iro; password=Secret123" Npgsql.EntityFrameworkCore.PostgreSQL -o Data -c InstantRunoffContext
```

Open up the `Data/InstantRunoffContext.cs` file and cut the connection string into your clipboard (the string that starts with `"host=..."`).  With the connection string in the clipboard, delete the entire OnConfiguring() method from `Data/InstantRunoffContext.cs`

Open up `appsettings.json` and add a `connectionStrings` section.  Paste in the connection string that you cut into your clipboard from the previous step.  It should look something similar to this:

```json appsettings.json
{
  "Logging": {
    "LogLevel": {
      "Default": "Information",
      "Microsoft.AspNetCore": "Warning"
    }
  },
  "AllowedHosts": "*",
  "ConnectionStrings": {
    "iro": "host=localhost; database=iro; user id=iro; password=Secret123"
  }
}

```

Now you're ready to modify your Program.cs to enable talking to the database

```csharp
using iro.web.Data;
using Microsoft.EntityFrameworkCore;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
builder.Services.AddRazorPages();
builder.Services.AddDbContext<InstantRunoffContext>(options => options.UseNpgsql(builder.Configuration.GetConnectionString("iro")));
```

To verify the code is working correctly and that the program can access the database, try adding the following line to the end of your `Program.cs` file (right before the `app.Run()` line)

```csharp
app.MapGet("/test", async (InstantRunoffContext context) => await context.Cities.ToListAsync());
```

Start up your app with `dotnet run` then open a browser and go to http://localhost:1234/test (where `1234` is the port number assigned to your app, it will show up in the console when you execute `dotnet run`).

Your browser should show something similar to:

```json
[
  {
    "id": 1,
    "cityName": "Manti City",
    "cityDescription": "",
    "contactTitle": "City Recorder",
    "contactName": "JoAnn Otten",
    "contactEmail": "mantiadmin@mail.manti.com",
    "contactPhone": "435-835-2401",
    "offices": []
  },
  {
    "id": 2,
    "cityName": "Ephraim City",
    "cityDescription": "",
    "contactTitle": "City Recorder",
    "contactName": "Leigh Ann Warnock",
    "contactEmail": "leighann.warnock@ephraimcity.org",
    "contactPhone": "435-283-4631",
    "offices": []
  }
]
```
  </details>
