using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;

public class MaterialMasterMigration : MigrationService
{
    private readonly string _selectQuery = "SELECT ITEMID, ITEMCODE, ITEMNAME, ITEMDESCRIPTION, UOMId, MaterialGroupId, ClientSAPId FROM TBL_ITEMMASTER";
    private readonly string _insertQuery = @"INSERT INTO material_master (material_id, material_code, material_name, material_description, uom_id, material_group_id, company_id, created_by, created_date, modified_by, modified_date, is_deleted, deleted_by, deleted_date) 
                                             VALUES (@material_id, @material_code, @material_name, @material_description, @uom_id, @material_group_id, @company_id, @created_by, @created_date, @modified_by, @modified_date, @is_deleted, @deleted_by, @deleted_date)";

    public MaterialMasterMigration(IConfiguration configuration) : base(configuration) { }

    public List<object> GetMappings()
    {
        // Parse sources from SELECT
        var sources = ParseSelectColumns(_selectQuery);
        // Add defaults for created_by, created_date, modified_by, modified_date, is_deleted, deleted_by, deleted_date
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
        var logics = new List<string> { "Direct", "Direct", "Direct", "Direct", "FK", "FK", "FK", "Default: 0", "Default: Now", "Default: null", "Default: null", "Default: false", "Default: null", "Default: null" };

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
            // Skip records where any FK is 0 to avoid constraint violations
            if ((int)reader["UOMId"] == 0 || (int)reader["MaterialGroupId"] == 0) continue;

            pgCmd.Parameters.Clear();
            pgCmd.Parameters.AddWithValue("@material_id", reader["ITEMID"]);
            pgCmd.Parameters.AddWithValue("@material_code", reader["ITEMCODE"]);
            pgCmd.Parameters.AddWithValue("@material_name", reader["ITEMNAME"]);
            pgCmd.Parameters.AddWithValue("@material_description", reader["ITEMDESCRIPTION"]);
            pgCmd.Parameters.AddWithValue("@uom_id", reader["UOMId"]);
            pgCmd.Parameters.AddWithValue("@material_group_id", reader["MaterialGroupId"]);
            pgCmd.Parameters.AddWithValue("@company_id", reader["ClientSAPId"]);
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