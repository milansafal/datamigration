using Microsoft.AspNetCore.Mvc;
using System.Collections.Generic;
using System.Threading.Tasks;

[Route("Migration")]
public class MigrationController : Controller
{
    private readonly UOMMasterMigration _uomMigration;
    private readonly PlantMasterMigration _plantMigration;
    private readonly CurrencyMasterMigration _currencyMigration;
    private readonly MaterialGroupMasterMigration _materialGroupMigration;
    private readonly PurchaseGroupMasterMigration _purchaseGroupMigration;
    private readonly PaymentTermMasterMigration _paymentTermMigration;
    private readonly MaterialMasterMigration _materialMigration;

    public MigrationController(
        UOMMasterMigration uomMigration, 
        PlantMasterMigration plantMigration,
        CurrencyMasterMigration currencyMigration,
        MaterialGroupMasterMigration materialGroupMigration,
        PurchaseGroupMasterMigration purchaseGroupMigration,
        PaymentTermMasterMigration paymentTermMigration,
        MaterialMasterMigration materialMigration)
    {
        _uomMigration = uomMigration;
        _plantMigration = plantMigration;
        _currencyMigration = currencyMigration;
        _materialGroupMigration = materialGroupMigration;
        _purchaseGroupMigration = purchaseGroupMigration;
        _paymentTermMigration = paymentTermMigration;
        _materialMigration = materialMigration;
    }

    public IActionResult Index()
    {
        return View();
    }

    [HttpGet("GetTables")]
    public IActionResult GetTables()
    {
        var tables = new List<object>
        {
            new { name = "uom", description = "TBL_UOM_MASTER to uom_master" },
            new { name = "plant", description = "TBL_PlantMaster to plant_master" },
            new { name = "currency", description = "TBL_CURRENCYMASTER to currency_master" },
            new { name = "materialgroup", description = "TBL_MaterialGroupMaster to material_group_master" },
            new { name = "purchasegroup", description = "TBL_PurchaseGroupMaster to purchase_group_master" },
            new { name = "paymentterm", description = "TBL_PAYMENTTERMMASTER to payment_term_master" },
            new { name = "material", description = "TBL_ITEMMASTER to material_master" }


        };
        return Json(tables);
    }

    [HttpGet("GetMappings")]
    public IActionResult GetMappings(string table)
    {
        if (table.ToLower() == "uom")
        {
            var mappings = _uomMigration.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "plant")
        {
            var mappings = _plantMigration.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "currency")
        {
            var mappings = _currencyMigration.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "materialgroup")
        {
            var mappings = _materialGroupMigration.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "purchasegroup")
        {
            var mappings = _purchaseGroupMigration.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "paymentterm")
        {
            var mappings = _paymentTermMigration.GetMappings();
            return Json(mappings);
        }
        else if (table.ToLower() == "material")
        {
            var mappings = _materialMigration.GetMappings();
            return Json(mappings);
        }
        return Json(new List<object>());
    }

    [HttpPost("MigrateAsync")]
    public async Task<IActionResult> MigrateAsync([FromBody] MigrationRequest request)
    {
        try
        {
            int recordCount = 0;
            if (request.Table.ToLower() == "uom")
            {
                recordCount = await _uomMigration.MigrateAsync();
            }
            else if (request.Table.ToLower() == "plant")
            {
                recordCount = await _plantMigration.MigrateAsync();
            }
            else if (request.Table.ToLower() == "currency")
            {
                recordCount = await _currencyMigration.MigrateAsync();
            }
            else if (request.Table.ToLower() == "materialgroup")
            {
                recordCount = await _materialGroupMigration.MigrateAsync();
            }
            else if (request.Table.ToLower() == "purchasegroup")
            {
                recordCount = await _purchaseGroupMigration.MigrateAsync();
            }
            else if (request.Table.ToLower() == "paymentterm")
            {
                recordCount = await _paymentTermMigration.MigrateAsync();
            }
            else if (request.Table.ToLower() == "material")
            {
                recordCount = await _materialMigration.MigrateAsync();
            }
            else
            {
                return Json(new { success = false, error = "Unknown table" });
            }
            return Json(new { success = true, message = $"Migration completed for {request.Table}. {recordCount} records migrated." });
        }
        catch (Exception ex)
        {
            return Json(new { success = false, error = ex.Message });
        }
    }
}

public class MigrationRequest
{
    public required string Table { get; set; }
}