# TB EMS 統計程式

| 統計類型       | 能源類型        | 資料類型       | 程式名稱        | 說明          |
| ------------- | ------------- | ------------- | ------------- | ------------- |
| 分統計 | 電力  | 耗用量 | * [sp_update_energy_stat_min_main()](Scripts/sp_update_energy_stat_min_main.sql) <br> * [sp_update_energy_stat_min_sub(IN p_device_id TEXT)](Scripts/sp_update_energy_stat_min_sub.sql) | 分統計沒有瞬間量。 |
| 分統計 | 非電  | 耗用量 | * [sp_update_other_energy_stat_min_main()](Scripts/sp_update_other_energy_stat_min_main.sql) <br> * [sp_update_other_energy_stat_min_sub(IN p_device_id TEXT)](Scripts/sp_update_other_energy_stat_min_sub.sql) | 分統計沒有瞬間量。 |
| 時統計 | 電力  | 耗用量 | * [sp_update_energy_stat_hour_main()](Scripts/sp_update_energy_stat_hour_main.sql) <br> * [sp_update_energy_stat_hour_sub(IN p_device_id TEXT)](Scripts/sp_update_energy_stat_hour_sub.sql) | 時統計沒有瞬間量。 |
| 時統計 | 非電  | 耗用量 | * [sp_update_other_energy_stat_hour_main()](Scripts/sp_update_other_energy_stat_hour_main.sql) <br> * [sp_update_other_energy_stat_hour_sub(IN p_device_id TEXT)](Scripts/sp_update_other_energy_stat_hour_sub.sql) | 時統計沒有瞬間量。 |
| 日統計 | 電力  | 耗用量 | * [sp_update_energy_stat_day_main()](Scripts/sp_update_energy_stat_day_main.sql) <br> * [sp_update_energy_stat_day_sub(IN p_device_id TEXT)](Scripts/sp_update_energy_stat_day_sub.sql) |  |
| 日統計 | 電力  | 瞬間量 | * [sp_update_energy_rate_day_main()](Scripts/sp_update_energy_rate_day_main.sql) <br> * [sp_update_energy_rate_day_sub(IN p_device_id TEXT)](Scripts/sp_update_energy_rate_day_sub.sql) |  |
| 日統計 | 非電  | 耗用量 | * [sp_update_other_energy_stat_day_main()](Scripts/sp_update_other_energy_stat_day_main.sql) <br> * [sp_update_other_energy_stat_day_sub(IN p_device_id TEXT)](Scripts/sp_update_other_energy_stat_hour_sub.sql) |  |
| 日統計 | 非電  | 瞬間量 | * [sp_update_other_energy_rate_day_main()](Scripts/sp_update_other_energy_rate_day_main.sql) <br> * [sp_update_other_energy_rate_day_sub(IN p_device_id TEXT)](Scripts/sp_update_other_energy_rate_day_sub.sql) |  |
| 月統計 | 電力  | 耗用量 & 瞬間量 | * [sp_update_energy_stat_mon_main()](Scripts/sp_update_energy_stat_mon_main.sql) <br> * [sp_update_energy_stat_mon_sub(IN p_device_id TEXT)](Scripts/sp_update_energy_stat_day_sub.sql) |  |
| 月統計 | 非電  | 耗用量 & 瞬間量 | * [sp_update_other_energy_stat_mon_main()](Scripts/sp_update_other_energy_stat_mon_main.sql) <br> * [sp_update_other_energy_stat_mon_sub(IN p_device_id TEXT)](Scripts/sp_update_other_energy_stat_mon_sub.sql) |  |
| 年統計 | 電力  | 耗用量 & 瞬間量 | * [sp_update_energy_stat_year_main()](Scripts/sp_update_energy_stat_year_main.sql) <br> * [sp_update_energy_stat_year_sub(IN p_device_id TEXT)](Scripts/sp_update_energy_stat_year_sub.sql) |  |
| 年統計 | 非電  | 耗用量 & 瞬間量 | * [sp_update_other_energy_stat_year_main()](Scripts/sp_update_other_energy_stat_year_main.sql) <br> * [sp_update_other_energy_stat_year_sub(IN p_device_id TEXT)](Scripts/sp_update_other_energy_stat_year_sub.sql) |  |

