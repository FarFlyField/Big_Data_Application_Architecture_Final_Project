CREATE EXTERNAL TABLE IF NOT EXISTS wuh_fifa_team_stats_hive (
    team STRING,
    best_rank INT,
    worst_rank INT,
    avg_abs_rank_change DOUBLE,
    first_year_active INT,
    last_year_active INT,
    total_years_ranked INT,
    confed_avg_points DOUBLE
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
    "hbase.columns.mapping" = 
        ":key,stats:best_rank,stats:worst_rank,stats:avg_abs_rank_change,stats:first_year_active,stats:last_year_active,stats:total_years_ranked,stats:confed_avg_points"
)
TBLPROPERTIES ("hbase.table.name" = "wuh_fifa_team_stats");

INSERT OVERWRITE TABLE wuh_fifa_team_stats_hive
SELECT 
    team,
    best_rank,
    worst_rank,
    avg_abs_rank_change,
    first_year_active,
    last_year_active,
    total_years_ranked,
    confed_avg_points
FROM wuh_fifa_team_batch_stats;
