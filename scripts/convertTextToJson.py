import os
import json

# Define the base directories
source_dir = '../data_crawled'  # Path to the folder containing the text files
target_dir = '../json_output'  # Path to the folder where JSON files will be saved


def parse_txt_content(file_content):
    data = {}
    for line in file_content.splitlines():
        if ':' in line:
            key, value = line.split(':', 1)
            data[key.strip()] = value.strip()
    return data


# Function to convert .txt files to .json and maintain folder structure
def convert_txt_to_json(source_folder, target_folder):
    for root, dirs, files in os.walk(source_folder):
        # Create a parallel folder structure in the target directory
        relative_path = os.path.relpath(root, source_folder)
        target_subfolder = os.path.join(target_folder, relative_path)

        if not os.path.exists(target_subfolder):
            os.makedirs(target_subfolder)

        for file_name in files:
            if file_name.endswith('.txt'):
                source_file_path = os.path.join(root, file_name)
                target_file_name = os.path.splitext(file_name)[0] + '.json'
                target_file_path = os.path.join(target_subfolder, target_file_name)

                # Read and parse the text file
                with open(source_file_path, 'r', encoding='utf-8') as f:
                    file_content = f.read()
                    parsed_data = parse_txt_content(file_content)


                with open(target_file_path, 'w', encoding='utf-8') as f:
                    json.dump(parsed_data, f, indent=4)

                print(f"Converted {source_file_path} to {target_file_path}")

# Convert the files
convert_txt_to_json(source_dir, target_dir)
