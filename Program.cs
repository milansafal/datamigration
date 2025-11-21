var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
builder.Services.AddControllersWithViews();
builder.Services.AddCors(options =>
{
    options.AddDefaultPolicy(builder =>
    {
        builder.AllowAnyOrigin()
               .AllowAnyMethod()
               .AllowAnyHeader();
    });
});
builder.Services.AddScoped<MigrationService>();
builder.Services.AddScoped<UOMMasterMigration>();
builder.Services.AddScoped<PlantMasterMigration>();
builder.Services.AddScoped<CurrencyMasterMigration>();
builder.Services.AddScoped<MaterialGroupMasterMigration>();
builder.Services.AddScoped<PurchaseGroupMasterMigration>();
builder.Services.AddScoped<PaymentTermMasterMigration>();
builder.Services.AddScoped<MaterialMasterMigration>();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (!app.Environment.IsDevelopment())
{
    app.UseExceptionHandler("/Home/Error");
    app.UseHsts();
}

app.UseHttpsRedirection();
app.UseStaticFiles();

app.UseRouting();

app.UseCors();

app.UseAuthorization();

// Map controllers
app.MapControllers();

// Default route to Migration/Index
app.MapControllerRoute(
    name: "default",
    pattern: "{controller=Migration}/{action=Index}/{id?}");

app.Run();