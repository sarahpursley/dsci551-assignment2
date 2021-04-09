#!/bin/bash
# Image caption script
# Run with command:
# ./step9_image_captions.sh
# in the terminal
# Takes about ~20 minutes to run

# Change to directory of the images on your machine (make sure they're all unzipped in one folder)
DIR=$PWD/Images

declare -a files
files=($DIR/*)
pos=$(( ${#files[*]} - 1 ))
last=${files[$pos]}

printf "[" >> img_captions.json

for FILE in "${files[@]}"
do
    if [[ $FILE == $last ]]
    then
        printf "Captioning image at $FILE"
        printf "$FILE is the last"

        printf "{\"$FILE\":" >> img_captions.json
        curl -X POST "localhost:8764/inception/v3/caption/image" --data-binary @${FILE} >> img_captions.json
        printf "}" >> img_captions.json
    else
        printf "Captioning image at $FILE"

        printf "{\"$FILE\":" >> img_captions.json
        curl -X POST "localhost:8764/inception/v3/caption/image" --data-binary @${FILE} >> img_captions.json
        printf "}," >> img_captions.json
    fi
done

printf "]" >> img_captions.json