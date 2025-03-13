import os
import subprocess
from datetime import datetime, timedelta

# Đường dẫn thư mục chứa JSON gốc
source_folder = "C:\\Users\\kkagi\\Downloads\\CarInsight-End-to-End-Data-Engineering-for-Used-Cars\\json_output"

# Đường dẫn Data Lake trên HDFS
hdfs_root = "/data_lake/raw/cars"

start_date = datetime(2025, 3, 2)

# Hàm kiểm tra và tạo thư mục trên HDFS nếu chưa tồn tại
def ensure_hdfs_directory(hdfs_path):
    try:
        # Kiểm tra xem thư mục đã tồn tại chưa bằng ls thay vì test
        print(f"Checking if HDFS directory exists: {hdfs_path}")
        check_cmd = ["hdfs.cmd", "dfs", "-ls", hdfs_path]
        result = subprocess.run(check_cmd, capture_output=True, text=True)

        if "No such file or directory" in result.stderr or result.returncode != 0:
            print(f"Creating HDFS directory: {hdfs_path}")
            mkdir_cmd = ["hdfs.cmd", "dfs", "-mkdir", "-p", hdfs_path]
            subprocess.run(mkdir_cmd, capture_output=True, text=True, check=True)
            print(f"Created HDFS directory: {hdfs_path}")
        else:
            print(f"HDFS directory {hdfs_path} already exists.")
    except subprocess.CalledProcessError as e:
        print(f"Error creating HDFS directory {hdfs_path}: {e.stderr}")
        raise

# Hàm để upload file từ local lên HDFS
def upload_to_hdfs(local_path, hdfs_path):
    if not os.path.exists(local_path):
        raise FileNotFoundError(f"Local file not found: {local_path}")

    print(f"Uploading: {local_path} -> {hdfs_path}")
    try:
        # Xóa file cũ nếu đã tồn tại để tránh lỗi
        rm_cmd = ["hdfs.cmd", "dfs", "-rm", hdfs_path]
        subprocess.run(rm_cmd, capture_output=True, text=True)

        # Upload file
        put_cmd = ["hdfs.cmd", "dfs", "-put", local_path, hdfs_path]
        result = subprocess.run(put_cmd, capture_output=True, text=True, check=True)

        # Kiểm tra xem file đã thực sự được upload chưa
        ls_cmd = ["hdfs.cmd", "dfs", "-ls", hdfs_path]
        ls_result = subprocess.run(ls_cmd, capture_output=True, text=True, check=True)
        if hdfs_path not in ls_result.stdout:
            raise Exception(f"File {hdfs_path} not found on HDFS after upload.")
        print(f"Successfully uploaded: {hdfs_path}")
    except subprocess.CalledProcessError as e:
        print(f"Error uploading file: {e.stderr}")
        raise

# Kiểm tra kết nối HDFS trước khi bắt đầu
try:
    print("Checking HDFS connection...")
    ls_result = subprocess.run(["hdfs.cmd", "dfs", "-ls", "/"], capture_output=True, text=True, check=True)
    print("HDFS root directory contents:")
    print(ls_result.stdout)
except subprocess.CalledProcessError as e:
    print(f"Error: Cannot connect to HDFS. Ensure Hadoop is running. Error: {e.stderr}")
    exit(1)

# Xử lý 50 folder, mỗi 5 folder là một partition
for group in range(10):  # 10 nhóm, mỗi nhóm 5 folder
    partition_date = start_date - timedelta(weeks=group)
    date_str = partition_date.strftime('%Y/%m/%d')
    hdfs_partition = f"{hdfs_root}/year_{date_str[:4]}/month_{date_str[5:7]}/day_{date_str[8:]}"

    # Đảm bảo thư mục partition tồn tại trên HDFS
    ensure_hdfs_directory(hdfs_partition)

    # Xử lý 5 folder trong nhóm hiện tại
    for folder_idx in range(5):
        folder_num = group * 5 + folder_idx + 1
        subdir_path = os.path.join(source_folder, str(folder_num))

        if os.path.isdir(subdir_path):
            json_files = [f for f in os.listdir(subdir_path) if f.endswith(".json")]

            if len(json_files) < 30:
                print(f"Warning: Folder {folder_num} has fewer than 30 JSON files ({len(json_files)} files)")

            for file in json_files:
                local_file_path = os.path.join(subdir_path, file)
                hdfs_file_path = f"{hdfs_partition}/{folder_num}_{file}"
                upload_to_hdfs(local_file_path, hdfs_file_path)
        else:
            print(f"Thư mục không tồn tại: {subdir_path}")

print("All JSON files successfully uploaded and partitioned!")