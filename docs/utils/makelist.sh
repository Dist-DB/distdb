items=(bin binary case cast coalesce connection_id conv convert current_user database if ifnull isnull last_insert_id nullif session_user system_user user version)
for item in "${items[@]}"; do
    cp ascii.rs "${item}.rs"
done