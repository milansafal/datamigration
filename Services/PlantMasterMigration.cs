using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;

public class PlantMasterMigration : MigrationService
{
    private readonly string _selectQuery = "SELECT PlantId, ClientSAPId, PlantCode, PlantName, CompanyCode FROM TBL_PlantMaster";
    private readonly string _insertQuery = @"INSERT INTO plant_master (plant_id, company_id, plant_code, plant_name, plant_company_code, created_by, created_date, modified_by, modified_date, is_deleted, deleted_by, deleted_date) 
                                             VALUES (@plant_id, @company_id, @plant_code, @plant_name, @plant_company_code, @created_by, @created_date, @modified_by, @modified_date, @is_deleted, @deleted_by, @deleted_date)";

    public PlantMasterMigration(IConfiguration configuration) : base(configuration) { }

    public List<object> GetMappings()
    {
        // Parse sources from SELECT
        var sources = ParseSelectColumns(_selectQuery);
        // Add defaults for modified_by, modified_date, is_deleted, deleted_by, deleted_date
        sources.Add("-");
        sources.Add("-");
        sources.Add("-");
        sources.Add("-");
        sources.Add("-");

        // Parse targets from INSERT
        var targets = ParseInsertColumns(_insertQuery);

        // Define logics - adjust as needed
        var logics = new List<string> { "Direct", "FK", "Direct", "Direct", "Direct", "Default: 0", "Default: Now", "Default: null", "Default: null", "Default: false", "Default: null", "Default: null" };

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
            pgCmd.Parameters.AddWithValue("@plant_id", reader["PlantId"]);
            pgCmd.Parameters.AddWithValue("@company_id", reader["ClientSAPId"]);
            pgCmd.Parameters.AddWithValue("@plant_code", reader["PlantCode"]);
            pgCmd.Parameters.AddWithValue("@plant_name", reader["PlantName"]);
            pgCmd.Parameters.AddWithValue("@plant_company_code", reader["CompanyCode"]);
            pgCmd.Parameters.AddWithValue("@created_by", 0);
            pgCmd.Parameters.AddWithValue("@created_date", DateTime.UtcNow);
            pgCmd.Parameters.AddWithValue("@modified_by", DBNull.Value);
            pgCmd.Parameters.AddWithValue("@modified_date", DBNull.Value);
            pgCmd.Parameters.AddWithValue("@is_deleted", false);
            pgCmd.Parameters.AddWithValue("@deleted_by", DBNull.Value);
            pgCmd.Parameters.AddWithValue("@deleted_date", DBNull.Value);
            int result = await pgCmd.ExecuteNonQueryAsync();
            if (result > 0) insertedCount++;
        }
        return insertedCount;
    }
}