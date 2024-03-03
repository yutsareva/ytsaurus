declare -A unique_lines
declare -A file_counts

for file in /tmp/fuzzing_artifacts/*; do
    output=$(/home/yutsareva/yt/build/yt/yt/server/all/fuzztests/protobuf-reader/protobuf-reader "$file" | head -n 1)
    if [[ -z "${unique_lines["$output"]}" ]]; then
        unique_lines["$output"]="$file"
        file_counts["$output"]=1
    else
        ((file_counts["$output"]++))
    fi
done

for line in "${!unique_lines[@]}"; do
    printf "Request: %s\nCount: %d\nFile sample: %s\n\n" "$line" "${file_counts["$line"]}" "${unique_lines["$line"]}"
done
