/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN

declare @start_time datetime,@end_time datetime,@start_time_bronze datetime,@end_time_bronze datetime
    begin try
        PRINT '===============================';
        PRINT 'loading bronze layer';
        PRINT '===============================';

        PRINT '------------------------------';
        PRINT 'loading crm tables';
        PRINT '------------------------------';

        set @start_time_bronze=GETDATE();
        set @start_time=GETDATE();
        PRINT '>> truncating table: bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;

        PRINT '>> inserting data into bronze.crm_cust_info';
        BULK INSERT bronze.crm_cust_info
        FROM 'C:\Users\hp\Desktop\BI project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        set @end_time=GETDATE();
        print'>>load duration ;'+cast (datediff(second,@start_time,@end_time)as nvarchar)+ ' seconds';
        print'>>---------------------------------------------------------------------';



        set @start_time=GETDATE();
        PRINT '>> truncating table: bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;

        PRINT '>> inserting data into bronze.crm_prd_info';
        BULK INSERT bronze.crm_prd_info
        FROM 'C:\Users\hp\Desktop\BI project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
                set @end_time=GETDATE();
        print'>>load duration ;'+cast (datediff(second,@start_time,@end_time)as nvarchar)+ ' seconds';
        print'>>---------------------------------------------------------------------';



          set @start_time=GETDATE();
        PRINT '>> truncating table: bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;

        PRINT '>> inserting data into bronze.crm_sales_details';
        BULK INSERT bronze.crm_sales_details
        FROM 'C:\Users\hp\Desktop\BI project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
                set @end_time=GETDATE();
        print'>>load duration ;'+cast (datediff(second,@start_time,@end_time)as nvarchar)+ ' seconds';
        print'>>---------------------------------------------------------------------';


        PRINT '------------------------------';
        PRINT 'loading erp tables';
        PRINT '------------------------------';

        set @start_time=GETDATE();
        PRINT '>> truncating table: bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;

        PRINT '>> inserting data into bronze.erp_cust_az12';
        BULK INSERT bronze.erp_cust_az12
        FROM 'C:\Users\hp\Desktop\BI project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
                set @end_time=GETDATE();
        print'>>load duration ;'+cast (datediff(second,@start_time,@end_time)as nvarchar)+ ' seconds';
        print'>>---------------------------------------------------------------------';



        set @start_time=GETDATE();
        PRINT '>> truncating table: bronze.erp_loc_101';
        TRUNCATE TABLE bronze.erp_loc_101;

        PRINT '>> inserting data into bronze.erp_loc_101';
        BULK INSERT bronze.erp_loc_101
        FROM 'C:\Users\hp\Desktop\BI project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        set @end_time=GETDATE();
        print'>>load duration ;'+cast (datediff(second,@start_time,@end_time)as nvarchar)+ ' seconds';
        print'>>---------------------------------------------------------------------';




          set @start_time=GETDATE();
        PRINT '>> truncating table: bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        PRINT '>> inserting data into bronze.erp_px_cat_g1v2';
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'C:\Users\hp\Desktop\BI project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        set @end_time=GETDATE();
        print'>>load duration ;'+cast (datediff(second,@start_time,@end_time)as nvarchar)+ ' seconds';
        print'>>---------------------------------------------------------------------';

        set @end_time_bronze=GETDATE();
        print'>>load bronze duration  ; '+cast (datediff(second,@start_time,@end_time)as nvarchar)+ ' seconds';
        print'>>---------------------------------------------------------------------';


    end try
    begin catch
     print'================================';
     print'error occured during loading bronze layer'
     print'error message'+error_message();
     print'error code'+cast (error_number() as nvarchar);
     print'error state'+cast (error_state() as nvarchar);
     print'================================'; 
    end catch;
END;
