#!/bin/bash

# MySQL Connection Details
DB_USER="root"
DB_PASSWORD="MTA3OTYtZmF3YWRh"
DB_NAME="sales"
TABLE_NAME="sales_data"

# Output File
OUTPUT_FILE="sales_data.sql"

# mysqldump command to export data
mysqldump -u "$DB_USER" -p"$DB_PASSWORD" "$DB_NAME" "$TABLE_NAME" > "$OUTPUT_FILE"

# Check for success
if [ $? -eq 0 ]; then
    echo "Data exported successfully to $OUTPUT_FILE"
else
    echo "Error exporting data. Please check the script and try again."
fi
