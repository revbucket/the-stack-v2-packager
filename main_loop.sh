#!/bin/bash

# Check if correct number of arguments provided
if [ "$#" -lt 3 ]; then
    echo "Usage: $0 <input_file> <i> <j>"
    echo "Process lines where line_number mod j equals i"
    echo "Example: $0 input.txt 2 3 (processes lines 2, 5, 8, ...)"
    exit 1
fi

input_file="$1"
i=$2
j=$3

# Validate modulo parameters
if [ "$j" -le 0 ]; then
    echo "Error: j must be positive"
    exit 1
fi

if [ "$i" -ge "$j" ]; then
    echo "Error: i must be less than j"
    exit 1
fi

total_lines=$(wc -l < "$input_file")
current_line=0
processed_lines=0

# Calculate how many lines will be processed
expected_lines=$(( (total_lines + j - i - 1) / j ))

draw_progress_bar() {
    local percentage=$1
    local description="$2"
    local width=50
    local filled=$(printf "%.0f" $(echo "$percentage * $width / 100" | bc -l))
    local empty=$((width - filled))
    
    # Calculate the total length of the progress bar line
    local total_length=$((${#description} + width + 50))  # 50 extra chars for brackets, percentage, etc.
    
    # Clear the entire line with spaces
    printf "\r%${total_length}s" " "
    
    # Move cursor back to start of line and draw the new progress bar
    printf "\r [" 
    printf "%${filled}s" '' | tr ' ' '#'
    printf "%${empty}s" '' | tr ' ' '-'
    printf "] %3d%% (%d/%d | Part %d/%d | %s)" \
        "$percentage" "$processed_lines" "$expected_lines" "$i" "$j" "$description"
}


# Process each line with progress bar
while IFS= read -r line; do
    ((current_line++))
    base_dir="/mnt/raid0"
    # Check if this line should be processed (line_number mod j equals i)
    # Note: We use current_line-1 because bash arrays are 0-based
    if [ $(( (current_line-1) % j )) -eq "$i" ]; then
        ((processed_lines++))
		
		# Step 1: Download all things with s5cmd 
		cmd_file="${line%.parquet}.cmd.txt"
		echo ""
        echo "Working on ${line}..."
		s5cmd run ${base_dir}/the-stack-v2/raw-hf-parquets/${cmd_file} > /dev/null
		lang=$(basename "$(dirname "$line")")

		./rust/target/release/rust process-parquet --parquet-file ${base_dir}/the-stack-v2/raw-hf-parquets/${line} --local-jsonl-dir ${base_dir}/jsonls/${lang}
		s5cmd sync ${base_dir}/jsonls/${lang}/ s3://ai2-llm/pretraining-data/sources/the-stack-v2/jsonl_data/${lang}/
		rm -rf ${base_dir}/the-stack-v2/data/
		rm -rf ${base_dir}/jsonls/${lang}
		echo ""
        #sleep 5.0
        # Calculate percentage based on processed lines
        percentage=$(printf "%.0f" $(echo "$processed_lines * 100 / $expected_lines" | bc -l))        
        # Update progress bar
        draw_progress_bar "$percentage" $line
        
    fi
    
done < "$input_file"

echo -e "\nProcessing complete! Processed $processed_lines lines matching pattern."