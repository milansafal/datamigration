using Microsoft.Data.SqlClient;
using Npgsql;
using System.Data;

public class EventMasterMigration : MigrationService
{
    public EventMasterMigration(IConfiguration configuration) : base(configuration)
    {
    }

    protected override string SelectQuery => @"
        SELECT 
            EVENTID, EVENTCODE, EVENTNAME, EVENTDESC, ROUND, EVENTTYPE, CURRENTSTATUS, 
            PARENTID, PRICINGSTATUS, ISEXTEND, EventCurrencyId, IschkIsSendMail, ClientSAPId,
            TechnicalApprovalSendDate, TechnicalApprovalApprovedDate, TechnicalApprovalStatus,
            EventMode, TiePreventLot, TiePreventItem, IsTargetPriceApplicable, 
            IsAutoExtendedEnable, NoofTimesAutoExtended, AutoExtendedMinutes, ApplyExtendedTimes,
            GREENPERCENTAGE, YELLOWPERCENTAGE, IsItemLevelRankShow, IsLotLevelRankShow,
            IsLotLevelAuction, IsBasicPriceApplicable, IsBasicPriceValidationReq, 
            IsMinMaxBidApplicable, IsLowestBidShow, BesideAuctionFirstBid, MinBid, MaxBid,
            LotLevelBasicPrice, IsPriceBidAttachmentcompulsory, IsDiscountApplicable,
            IsGSTCompulsory, IsTechnicalAttachmentcompulsory, IsProposedQty, IsRedyStockmandatory,
            MinBidMode, MaxBidMode
        FROM TBL_EVENTMASTER";

    protected override string InsertQuery => @"
        INSERT INTO event_master (
            event_id, event_code, event_name, event_description, round, event_type, 
            event_status, parent_id, price_bid_template, is_standalone, pricing_status, 
            event_extended, event_currency_id, disable_mail_in_next_round, company_id,
            technical_approval_send_date, technical_approval_approved_date, 
            technical_approval_status, created_by, created_date
        ) VALUES (
            @event_id, @event_code, @event_name, @event_description, @round, @event_type, 
            @event_status, @parent_id, @price_bid_template, @is_standalone, @pricing_status, 
            @event_extended, @event_currency_id, @disable_mail_in_next_round, @company_id,
            @technical_approval_send_date, @technical_approval_approved_date, 
            @technical_approval_status, @created_by, @created_date
        ) RETURNING event_id";

    private string InsertEventSettingQuery => @"
        INSERT INTO event_setting (
            event_id, event_mode, tie_prevent_lot, tie_prevent_item, target_price_applicable,
            auto_extended_enable, no_of_times_auto_extended, auto_extended_minutes, 
            apply_extended_times, green_percentage, yellow_percentage, show_item_level_rank,
            show_lot_level_rank, basic_price_applicable, basic_price_validation_mandatory,
            min_max_bid_applicable, show_lower_bid, apply_all_settings_in_price_bid,
            min_lot_auction_bid_value, max_lot_auction_bid_value, configure_lot_level_auction,
            lot_level_basic_price, price_bid_attachment_mandatory, discount_applicable,
            gst_mandatory, technical_attachment_mandatory, proposed_qty, ready_stock_mandatory,
            created_by, created_date, lot_level_target_price, max_lot_bid_type, 
            min_lot_bid_type, allow_currency_selection
        ) VALUES (
            @event_id, @event_mode, @tie_prevent_lot, @tie_prevent_item, @target_price_applicable,
            @auto_extended_enable, @no_of_times_auto_extended, @auto_extended_minutes,
            @apply_extended_times, @green_percentage, @yellow_percentage, @show_item_level_rank,
            @show_lot_level_rank, @basic_price_applicable, @basic_price_validation_mandatory,
            @min_max_bid_applicable, @show_lower_bid, @apply_all_settings_in_price_bid,
            @min_lot_auction_bid_value, @max_lot_auction_bid_value, @configure_lot_level_auction,
            @lot_level_basic_price, @price_bid_attachment_mandatory, @discount_applicable,
            @gst_mandatory, @technical_attachment_mandatory, @proposed_qty, @ready_stock_mandatory,
            @created_by, @created_date, @lot_level_target_price, @max_lot_bid_type,
            @min_lot_bid_type, @allow_currency_selection
        )";

    protected override List<string> GetLogics()
    {
        return new List<string>
        {
            "EVENTID -> event_id (Direct)",
            "EVENTCODE -> event_code (Direct)",
            "EVENTNAME -> event_name (Direct)",
            "EVENTDESC -> event_description (Direct)",
            "ROUND -> round (Direct)",
            "EVENTTYPE -> event_type (Direct)",
            "CURRENTSTATUS -> event_status (Direct)",
            "PARENTID -> parent_id (Direct, 0 if NULL)",
            "price_bid_template -> pb buyer table pbtype (Lookup)",
            "is_standalone -> 0 (Fixed)",
            "PRICINGSTATUS -> pricing_status (Direct)",
            "ISEXTEND -> event_extended (Direct)",
            "EventCurrencyId -> event_currency_id (Direct)",
            "IschkIsSendMail -> disable_mail_in_next_round (Direct)",
            "ClientSAPId -> company_id (Direct)",
            "TechnicalApprovalSendDate -> technical_approval_send_date (Direct)",
            "TechnicalApprovalApprovedDate -> technical_approval_approved_date (Direct)",
            "TechnicalApprovalStatus -> technical_approval_status (Direct)",
            "created_by -> 0 (Fixed)",
            "created_date -> NOW() (Generated)",
            "--- Event Setting Table ---",
            "event_id -> event_id (From event_master)",
            "EventMode -> event_mode (Direct)",
            "TiePreventLot -> tie_prevent_lot (Direct)",
            "TiePreventItem -> tie_prevent_item (Direct)",
            "IsTargetPriceApplicable -> target_price_applicable (Direct)",
            "IsAutoExtendedEnable -> auto_extended_enable (Direct)",
            "NoofTimesAutoExtended -> no_of_times_auto_extended (Direct)",
            "AutoExtendedMinutes -> auto_extended_minutes (Direct)",
            "ApplyExtendedTimes -> apply_extended_times (Direct)",
            "GREENPERCENTAGE -> green_percentage (Direct)",
            "YELLOWPERCENTAGE -> yellow_percentage (Direct)",
            "IsItemLevelRankShow -> show_item_level_rank (Direct)",
            "IsLotLevelRankShow -> show_lot_level_rank (Direct)",
            "basic_price_applicable -> IF(IsLotLevelAuction==1) THEN IsLotLevelAuction ELSE IsBasicPriceApplicable (Conditional)",
            "IsBasicPriceValidationReq -> basic_price_validation_mandatory (Direct)",
            "IsMinMaxBidApplicable -> min_max_bid_applicable (Direct)",
            "IsLowestBidShow -> show_lower_bid (Direct)",
            "BesideAuctionFirstBid -> apply_all_settings_in_price_bid (Direct)",
            "MinBid -> min_lot_auction_bid_value (Direct)",
            "MaxBid -> max_lot_auction_bid_value (Direct)",
            "IsLotLevelAuction -> configure_lot_level_auction (Direct)",
            "LotLevelBasicPrice -> lot_level_basic_price (Direct)",
            "IsPriceBidAttachmentcompulsory -> price_bid_attachment_mandatory (Direct)",
            "IsDiscountApplicable -> discount_applicable (Direct)",
            "IsGSTCompulsory -> gst_mandatory (Direct)",
            "IsTechnicalAttachmentcompulsory -> technical_attachment_mandatory (Direct)",
            "IsProposedQty -> proposed_qty (Direct)",
            "IsRedyStockmandatory -> ready_stock_mandatory (Direct)",
            "lot_level_target_price -> 0 (Fixed)",
            "MinBidMode -> max_lot_bid_type (Direct)",
            "MaxBidMode -> min_lot_bid_type (Direct)",
            "allow_currency_selection -> 0 (Fixed)"
        };
    }

    public async Task<int> MigrateAsync()
    {
        int successCount = 0;

        try
        {
            using var sqlConnection = GetSqlServerConnection();
            using var pgConnection = GetPostgreSqlConnection();

            await sqlConnection.OpenAsync();
            await pgConnection.OpenAsync();

            using var sqlCommand = new SqlCommand(SelectQuery, sqlConnection);
            using var reader = await sqlCommand.ExecuteReaderAsync();

            while (await reader.ReadAsync())
            {
                try
                {
                    using var transaction = await pgConnection.BeginTransactionAsync();

                    try
                    {
                        // Insert into event_master and get the generated event_id
                        int eventId;
                        using (var insertCmd = new NpgsqlCommand(InsertQuery, pgConnection, transaction))
                        {
                            // Event Master fields
                            insertCmd.Parameters.AddWithValue("@event_id", reader["EVENTID"]);
                            insertCmd.Parameters.AddWithValue("@event_code", reader["EVENTCODE"] ?? DBNull.Value);
                            insertCmd.Parameters.AddWithValue("@event_name", reader["EVENTNAME"] ?? DBNull.Value);
                            insertCmd.Parameters.AddWithValue("@event_description", reader["EVENTDESC"] ?? DBNull.Value);
                            insertCmd.Parameters.AddWithValue("@round", reader["ROUND"] != DBNull.Value ? reader["ROUND"] : 0);
                            insertCmd.Parameters.AddWithValue("@event_type", reader["EVENTTYPE"] ?? DBNull.Value);
                            insertCmd.Parameters.AddWithValue("@event_status", reader["CURRENTSTATUS"] ?? DBNull.Value);
                            insertCmd.Parameters.AddWithValue("@parent_id", reader["PARENTID"] != DBNull.Value ? reader["PARENTID"] : 0);
                            insertCmd.Parameters.AddWithValue("@price_bid_template", DBNull.Value); // TODO: Lookup from pb buyer table
                            insertCmd.Parameters.AddWithValue("@is_standalone", false);
                            insertCmd.Parameters.AddWithValue("@pricing_status", reader["PRICINGSTATUS"] != DBNull.Value ? Convert.ToBoolean(reader["PRICINGSTATUS"]) : false);
                            insertCmd.Parameters.AddWithValue("@event_extended", reader["ISEXTEND"] != DBNull.Value ? Convert.ToBoolean(reader["ISEXTEND"]) : false);
                            insertCmd.Parameters.AddWithValue("@event_currency_id", reader["EventCurrencyId"] != DBNull.Value ? reader["EventCurrencyId"] : 0);
                            insertCmd.Parameters.AddWithValue("@disable_mail_in_next_round", reader["IschkIsSendMail"] != DBNull.Value ? Convert.ToBoolean(reader["IschkIsSendMail"]) : false);
                            insertCmd.Parameters.AddWithValue("@company_id", reader["ClientSAPId"] != DBNull.Value ? reader["ClientSAPId"] : 0);
                            insertCmd.Parameters.AddWithValue("@technical_approval_send_date", reader["TechnicalApprovalSendDate"] ?? DBNull.Value);
                            insertCmd.Parameters.AddWithValue("@technical_approval_approved_date", reader["TechnicalApprovalApprovedDate"] ?? DBNull.Value);
                            insertCmd.Parameters.AddWithValue("@technical_approval_status", reader["TechnicalApprovalStatus"] ?? DBNull.Value);
                            insertCmd.Parameters.AddWithValue("@created_by", 0);
                            insertCmd.Parameters.AddWithValue("@created_date", DateTime.Now);

                            eventId = (int)await insertCmd.ExecuteScalarAsync();
                        }

                        // Insert into event_setting
                        using (var settingCmd = new NpgsqlCommand(InsertEventSettingQuery, pgConnection, transaction))
                        {
                            settingCmd.Parameters.AddWithValue("@event_id", eventId);
                            settingCmd.Parameters.AddWithValue("@event_mode", reader["EventMode"] ?? DBNull.Value);
                            settingCmd.Parameters.AddWithValue("@tie_prevent_lot", reader["TiePreventLot"] != DBNull.Value ? Convert.ToBoolean(reader["TiePreventLot"]) : false);
                            settingCmd.Parameters.AddWithValue("@tie_prevent_item", reader["TiePreventItem"] != DBNull.Value ? Convert.ToBoolean(reader["TiePreventItem"]) : false);
                            settingCmd.Parameters.AddWithValue("@target_price_applicable", reader["IsTargetPriceApplicable"] != DBNull.Value ? Convert.ToBoolean(reader["IsTargetPriceApplicable"]) : false);
                            settingCmd.Parameters.AddWithValue("@auto_extended_enable", reader["IsAutoExtendedEnable"] != DBNull.Value ? Convert.ToBoolean(reader["IsAutoExtendedEnable"]) : false);
                            settingCmd.Parameters.AddWithValue("@no_of_times_auto_extended", reader["NoofTimesAutoExtended"] != DBNull.Value ? reader["NoofTimesAutoExtended"] : 0);
                            settingCmd.Parameters.AddWithValue("@auto_extended_minutes", reader["AutoExtendedMinutes"] != DBNull.Value ? reader["AutoExtendedMinutes"] : 0);
                            settingCmd.Parameters.AddWithValue("@apply_extended_times", reader["ApplyExtendedTimes"] != DBNull.Value ? Convert.ToBoolean(reader["ApplyExtendedTimes"]) : false);
                            settingCmd.Parameters.AddWithValue("@green_percentage", reader["GREENPERCENTAGE"] != DBNull.Value ? reader["GREENPERCENTAGE"] : 0);
                            settingCmd.Parameters.AddWithValue("@yellow_percentage", reader["YELLOWPERCENTAGE"] != DBNull.Value ? reader["YELLOWPERCENTAGE"] : 0);
                            settingCmd.Parameters.AddWithValue("@show_item_level_rank", reader["IsItemLevelRankShow"] != DBNull.Value ? Convert.ToBoolean(reader["IsItemLevelRankShow"]) : false);
                            settingCmd.Parameters.AddWithValue("@show_lot_level_rank", reader["IsLotLevelRankShow"] != DBNull.Value ? Convert.ToBoolean(reader["IsLotLevelRankShow"]) : false);

                            // Conditional logic for basic_price_applicable
                            var isLotLevelAuction = reader["IsLotLevelAuction"] != DBNull.Value && Convert.ToBoolean(reader["IsLotLevelAuction"]);
                            if (isLotLevelAuction)
                            {
                                settingCmd.Parameters.AddWithValue("@basic_price_applicable", Convert.ToBoolean(reader["IsLotLevelAuction"]));
                            }
                            else
                            {
                                settingCmd.Parameters.AddWithValue("@basic_price_applicable", reader["IsBasicPriceApplicable"] != DBNull.Value ? Convert.ToBoolean(reader["IsBasicPriceApplicable"]) : false);
                            }

                            settingCmd.Parameters.AddWithValue("@basic_price_validation_mandatory", reader["IsBasicPriceValidationReq"] != DBNull.Value ? Convert.ToBoolean(reader["IsBasicPriceValidationReq"]) : false);
                            settingCmd.Parameters.AddWithValue("@min_max_bid_applicable", reader["IsMinMaxBidApplicable"] != DBNull.Value ? Convert.ToBoolean(reader["IsMinMaxBidApplicable"]) : false);
                            settingCmd.Parameters.AddWithValue("@show_lower_bid", reader["IsLowestBidShow"] != DBNull.Value ? Convert.ToBoolean(reader["IsLowestBidShow"]) : false);
                            settingCmd.Parameters.AddWithValue("@apply_all_settings_in_price_bid", reader["BesideAuctionFirstBid"] != DBNull.Value ? Convert.ToBoolean(reader["BesideAuctionFirstBid"]) : false);
                            settingCmd.Parameters.AddWithValue("@min_lot_auction_bid_value", reader["MinBid"] != DBNull.Value ? reader["MinBid"] : 0);
                            settingCmd.Parameters.AddWithValue("@max_lot_auction_bid_value", reader["MaxBid"] != DBNull.Value ? reader["MaxBid"] : 0);
                            settingCmd.Parameters.AddWithValue("@configure_lot_level_auction", reader["IsLotLevelAuction"] != DBNull.Value ? Convert.ToBoolean(reader["IsLotLevelAuction"]) : false);
                            settingCmd.Parameters.AddWithValue("@lot_level_basic_price", reader["LotLevelBasicPrice"] != DBNull.Value ? reader["LotLevelBasicPrice"] : 0);
                            settingCmd.Parameters.AddWithValue("@price_bid_attachment_mandatory", reader["IsPriceBidAttachmentcompulsory"] != DBNull.Value ? Convert.ToBoolean(reader["IsPriceBidAttachmentcompulsory"]) : false);
                            settingCmd.Parameters.AddWithValue("@discount_applicable", reader["IsDiscountApplicable"] != DBNull.Value ? Convert.ToBoolean(reader["IsDiscountApplicable"]) : false);
                            settingCmd.Parameters.AddWithValue("@gst_mandatory", reader["IsGSTCompulsory"] != DBNull.Value ? Convert.ToBoolean(reader["IsGSTCompulsory"]) : false);
                            settingCmd.Parameters.AddWithValue("@technical_attachment_mandatory", reader["IsTechnicalAttachmentcompulsory"] != DBNull.Value ? Convert.ToBoolean(reader["IsTechnicalAttachmentcompulsory"]) : false);
                            settingCmd.Parameters.AddWithValue("@proposed_qty", reader["IsProposedQty"] != DBNull.Value ? Convert.ToBoolean(reader["IsProposedQty"]) : false);
                            settingCmd.Parameters.AddWithValue("@ready_stock_mandatory", reader["IsRedyStockmandatory"] != DBNull.Value ? Convert.ToBoolean(reader["IsRedyStockmandatory"]) : false);
                            settingCmd.Parameters.AddWithValue("@created_by", 0);
                            settingCmd.Parameters.AddWithValue("@created_date", DateTime.Now);
                            settingCmd.Parameters.AddWithValue("@lot_level_target_price", 0);
                            settingCmd.Parameters.AddWithValue("@max_lot_bid_type", reader["MinBidMode"] ?? DBNull.Value);
                            settingCmd.Parameters.AddWithValue("@min_lot_bid_type", reader["MaxBidMode"] ?? DBNull.Value);
                            settingCmd.Parameters.AddWithValue("@allow_currency_selection", false);

                            await settingCmd.ExecuteNonQueryAsync();
                        }

                        await transaction.CommitAsync();
                        successCount++;
                    }
                    catch (Exception ex)
                    {
                        await transaction.RollbackAsync();
                    }
                }
                catch (Exception ex)
                {
                }   
                
            }

        }
        catch (Exception ex)
        {
        }

        return successCount;
    }
}
