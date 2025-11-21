using Microsoft.Data.SqlClient;
using Npgsql;
using Microsoft.Extensions.Configuration;

public class MigrationService
{
    private readonly IConfiguration _configuration;

    public MigrationService(IConfiguration configuration)
    {
        _configuration = configuration;
    }

    public SqlConnection GetSqlServerConnection()
    {
        return new SqlConnection(_configuration.GetConnectionString("SqlServer"));
    }

    public NpgsqlConnection GetPostgreSqlConnection()
    {
        return new NpgsqlConnection(_configuration.GetConnectionString("PostgreSql"));
    }
}