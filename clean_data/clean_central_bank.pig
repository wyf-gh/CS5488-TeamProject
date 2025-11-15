
central_data = LOAD 'bigdata.central_bank' 
    USING org.apache.hive.hcatalog.pig.HCatLoader(); 


central_processed_data = FILTER central_data  
    BY region != 'Eurozone' 
    AND datavalue IS NOT NULL 
    AND (datavalue > 0 OR datavalue < 0);  


STORE central_processed_data 
    INTO '/user/master/cleaned_central' 
    USING PigStorage(',');  