using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;

public class CurrencyMasterMigration : MigrationService
{
    private readonly string _selectQuery = "SELECT CurrencyMastID, Currency_Code, Currency_Name FROM TBL_CURRENCYMASTER";
    private readonly string _insertQuery = @"INSERT INTO currency_master (currency_id, company_id, currency_code, currency_name, currency_short_name, decimal_places, iso_code, created_by, created_date, modified_by, modified_date, is_deleted, deleted_by, deleted_date) 
                                             VALUES (@currency_id, @company_id, @currency_code, @currency_name, @currency_short_name, @decimal_places, @iso_code, @created_by, @created_date, @modified_by, @modified_date, @is_deleted, @deleted_by, @deleted_date)";

    public CurrencyMasterMigration(IConfiguration configuration) : base(configuration) { }

    public List<object> GetMappings()
    {
        // Parse sources from SELECT
        var sources = ParseSelectColumns(_selectQuery);
        // Add defaults for company_id, currency_short_name, decimal_places, iso_code, created_by, created_date, modified_by, modified_date, is_deleted, deleted_by, deleted_date
        sources.Insert(0, "-"); // company_id before currency_id
        sources.Add("-"); // currency_short_name
        sources.Add("-"); // decimal_places
        sources.Add("-"); // iso_code
        sources.Add("-"); // created_by
        sources.Add("-"); // created_date
        sources.Add("-"); // modified_by
        sources.Add("-"); // modified_date
        sources.Add("-"); // is_deleted
        sources.Add("-"); // deleted_by
        sources.Add("-"); // deleted_date

        // Parse targets from INSERT
        var targets = ParseInsertColumns(_insertQuery);

        // Define logics - adjust as needed
        var logics = new List<string> { "Default: 1", "Direct", "Direct", "Direct", "Default: null", "Default: null", "Default: null", "Default: 0", "Default: Now", "Default: null", "Default: null", "Default: false", "Default: null", "Default: null" };

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
            pgCmd.Parameters.AddWithValue("@currency_id", reader["CurrencyMastID"]);
            pgCmd.Parameters.AddWithValue("@company_id", 1); // Default: 1
            pgCmd.Parameters.AddWithValue("@currency_code", reader["Currency_Code"]);
            pgCmd.Parameters.AddWithValue("@currency_name", reader["Currency_Name"]);
            pgCmd.Parameters.AddWithValue("@currency_short_name", DBNull.Value); // Default: null
            pgCmd.Parameters.AddWithValue("@decimal_places", 2); // Changed to default: 2 to avoid NOT NULL constraint
            pgCmd.Parameters.AddWithValue("@iso_code", DBNull.Value); // Default: null
            pgCmd.Parameters.AddWithValue("@created_by", 0); // Default: 0
            pgCmd.Parameters.AddWithValue("@created_date", DateTime.UtcNow); // Default: Now
            pgCmd.Parameters.AddWithValue("@modified_by", DBNull.Value); // Default: null
            pgCmd.Parameters.AddWithValue("@modified_date", DBNull.Value); // Default: null
            pgCmd.Parameters.AddWithValue("@is_deleted", false); // Default: false
            pgCmd.Parameters.AddWithValue("@deleted_by", DBNull.Value); // Default: null
            pgCmd.Parameters.AddWithValue("@deleted_date", DBNull.Value); // Default: null
            int result = await pgCmd.ExecuteNonQueryAsync();
            if (result > 0) insertedCount++;
        }
        return insertedCount;
    }
}