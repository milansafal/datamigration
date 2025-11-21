using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;

public class UOMMasterMigration : MigrationService
{
    private readonly string _selectQuery = "SELECT UOM_MAST_ID, ClientSAPId, UOMCODE, UOMNAME FROM TBL_UOM_MASTER";
    private readonly string _insertQuery = @"INSERT INTO uom_master (uom_id, company_id, uom_code, uom_name, created_by, created_date) 
                                             VALUES (@uom_id, CASE WHEN @company_id IS NULL THEN NULL ELSE @company_id END, @uom_code, @uom_name, @created_by, @created_date)";

    public UOMMasterMigration(IConfiguration configuration) : base(configuration) { }

    public List<object> GetMappings()
    {
        // Parse sources from SELECT
        var sources = ParseSelectColumns(_selectQuery);
        // Add defaults for created_by and created_date
        sources.Add("-");
        sources.Add("-");

        // Parse targets from INSERT
        var targets = ParseInsertColumns(_insertQuery);

        // Define logics
        var logics = new List<string> { "Direct", "FK", "Direct", "Direct", "Default: 0", "Default: Now" };

        // Build mappings
        var mappings = new List<object>();
        for (int i = 0; i < sources.Count; i++)
        {
            mappings.Add(new { source = sources[i], logic = logics[i], target = targets[i] });
        }
        return mappings;
    }

    private List<string> ParseSelectColumns(string selectQuery)
    {
        // Simple parsing: assume "SELECT col1, col2, ... FROM table"
        var start = selectQuery.IndexOf("SELECT") + 7;
        var end = selectQuery.IndexOf("FROM");
        var columnsPart = selectQuery.Substring(start, end - start).Trim();
        return columnsPart.Split(',').Select(c => c.Trim()).ToList();
    }

    private List<string> ParseInsertColumns(string insertQuery)
    {
        // Simple parsing: assume "INSERT INTO table (col1, col2, ...) VALUES (...)"
        var start = insertQuery.IndexOf("(") + 1;
        var end = insertQuery.IndexOf(")");
        var columnsPart = insertQuery.Substring(start, end - start).Trim();
        return columnsPart.Split(',').Select(c => c.Trim()).ToList();
    }

    public async Task<int> MigrateAsync()
    {
        using var sqlConn = GetSqlServerConnection();
        using var pgConn = GetPostgreSqlConnection();
        await sqlConn.OpenAsync();
        await pgConn.OpenAsync();

        using var sqlCmd = new SqlCommand(_selectQuery, sqlConn);
        using var reader = await sqlCmd.ExecuteReaderAsync();

        using var pgCmd = new NpgsqlCommand(_insertQuery, pgConn);
        int insertedCount = 0;
        while (await reader.ReadAsync())
        {
            pgCmd.Parameters.Clear();
            pgCmd.Parameters.AddWithValue("@uom_id", reader["UOM_MAST_ID"]);
            pgCmd.Parameters.AddWithValue("@company_id", reader["ClientSAPId"]);
            pgCmd.Parameters.AddWithValue("@uom_code", reader["UOMCODE"]);
            pgCmd.Parameters.AddWithValue("@uom_name", reader["UOMNAME"]);
            pgCmd.Parameters.AddWithValue("@created_by", 0);
            pgCmd.Parameters.AddWithValue("@created_date", DateTime.UtcNow);
            int result = await pgCmd.ExecuteNonQueryAsync();
            if (result > 0) insertedCount++;
        }
        return insertedCount;
    }
}