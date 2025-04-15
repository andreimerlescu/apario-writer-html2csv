#!/bin/bash

# Check for correct number of arguments
if [ $# -ne 3 ]; then
    echo "Usage: $0 <pdf_directory> <existing_csv> <input_csv>"
    exit 1
fi

# Assign arguments
pdf_dir="$1"
existing_csv="$2"
input_csv="$3"

# Validate inputs
[ ! -d "$pdf_dir" ] && { echo "Error: Directory $pdf_dir does not exist."; exit 1; }
[ ! -f "$input_csv" ] && { echo "Error: $input_csv is not a file."; exit 1; }

# Resolve absolute path for PDF directory
dir_path=$(realpath "$pdf_dir")
# Define output file
output_file="${input_csv%.csv}-combined.csv"
temp_file=$(mktemp)

# Initialize variables
declare -A existing_entries
max_id=-1

# Process existing CSV if it exists and is non-empty
if [ -f "$existing_csv" ] && [ -s "$existing_csv" ]; then
    # Copy header and find max ID
    head -n 1 "$existing_csv" > "$temp_file"
    # Build map of URL to full row and find max ID
    while IFS=',' read -r id url path filename pages; do
        # Skip header
        [ "$id" = "ID" ] && continue
        existing_entries["$url"]="$id,$url,$path,$filename,$pages"
        # Extract numeric part of ID (e.g., 0000 from JFK-0000)
        id_num=${id#JFK-}
        id_num=$((10#$id_num))  # Convert to decimal, handle leading zeros
        [ $id_num -gt $max_id ] && max_id=$id_num
        # Append existing entry
        echo "$id,$url,$path,$filename,$pages" >> "$temp_file"
    done < "$existing_csv"
else
    # No existing CSV, start fresh
    echo "ID,URL,PATH,FILENAME,PAGES" > "$temp_file"
    max_id=-1
fi

# Set starting ID for new entries
next_id=$((max_id + 1))

# Process new input CSV
echo "Processing new entries..."
tail -n +2 "$input_csv" | tr -d '\r' | while IFS=',' read -r url path filename; do
    # Skip empty URL rows
    [ -z "$url" ] && { echo "Skipping empty URL: $url,$path,$filename"; continue; }

    # Check if URL already exists
    if [ -n "${existing_entries["$url"]}" ]; then
        echo "URL already exists, skipping: $url"
        continue
    fi

    # Assign new ID
    id=$(printf "JFK-%04d" $next_id)
    next_id=$((next_id + 1))

    # Extract page count
    pdf_path="$dir_path/$filename"
    if [ ! -f "$pdf_path" ]; then
        echo "PDF not found: $pdf_path"
        page_count="N/A"
    else
        page_count=$(pdfinfo "$pdf_path" | grep 'Pages:' | awk '{print $2}')
        [ -z "$page_count" ] && page_count="Unknown"
    fi

    # Append new entry
    printf "%s,%s,%s,%s,%s\n" "$id" "$url" "$path" "$filename" "$page_count" >> "$temp_file"
done

# Finalize output
mv "$temp_file" "$output_file"
echo "Created $output_file"