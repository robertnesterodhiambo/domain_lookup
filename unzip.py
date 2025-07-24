import os
import zipfile
import shutil

# === Setup folders ===
domain_zip_folder = 'domain_zip'
output_folder = 'looku_file'
temp_extract_folder = 'temp_extract'

# Create output and temp folders if they don't exist
os.makedirs(output_folder, exist_ok=True)
os.makedirs(temp_extract_folder, exist_ok=True)

# === Output file path ===
output_file_path = os.path.join(output_folder, 'lookup.txt')

# === Clear output file if exists ===
with open(output_file_path, 'w') as outfile:
    pass

# === Process each zip file ===
for filename in os.listdir(domain_zip_folder):
    if filename.endswith('.zip'):
        zip_path = os.path.join(domain_zip_folder, filename)
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(temp_extract_folder)
            print(f"Successfully unzipped: {filename}")

# === Merge extracted files into lookup.txt ===
with open(output_file_path, 'a') as outfile:
    for root, dirs, files in os.walk(temp_extract_folder):
        for file in files:
            file_path = os.path.join(root, file)
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as infile:
                outfile.write(infile.read())
                outfile.write('\n')  # Ensure each file content is separated

# === Cleanup temp folder and its files ===
shutil.rmtree(temp_extract_folder)
print("All unzipped files and temp folder deleted.")
print(f"All files merged into {output_file_path}")
