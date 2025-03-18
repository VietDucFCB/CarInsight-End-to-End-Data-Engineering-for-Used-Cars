# Dự Án Pipeline Dữ Liệu

## Tổng Quan

Dự án này xây dựng một hệ thống xử lý dữ liệu toàn diện cho lĩnh vực xe ô tô cũ, thể hiện vai trò và công việc của một Data Engineer chuyên nghiệp trong môi trường doanh nghiệp. Hệ thống tập trung vào việc thu thập, lưu trữ, xử lý và phân tích dữ liệu xe ô tô cũ từ nhiều nguồn khác nhau, nhằm cung cấp giải pháp tư vấn mua xe thông minh và dự đoán khả năng bán của từng mẫu xe.

Dự án kết hợp nhiều công nghệ Big Data hiện đại như Apache Hadoop, Apache Spark, Apache Kafka, và các hệ thống cơ sở dữ liệu phân tán nhằm xây dựng một pipeline dữ liệu hoàn chỉnh, tự động hóa toàn bộ quá trình từ thu thập đến phân tích dữ liệu.

## Mục Tiêu Dự Án
- Xây dựng hệ thống thu thập dữ liệu xe ô tô cũ tự động từ nhiều nguồn
- Thiết kế kiến trúc lưu trữ dữ liệu phân tán, hiệu quả và dễ mở rộng
- Phát triển các pipeline ETL tự động hóa cho việc xử lý dữ liệu
- Xây dựng hệ thống gợi ý xe phù hợp dựa trên nhu cầu người dùng
- Phát triển mô hình dự đoán tỷ lệ bán thành công của xe theo giá niêm yết
- Tạo ra các ứng dụng thực tế để tận dụng dữ liệu đã xử lý

## Tổ chức file:
```
project/
├── .gitignore
├── .idea/
├── DataLakeArchitecture.txt
├── Dockerfile
├── Dockerfile.airflow
├── Dockerfile.airflow-init
├── Dockerfile.kafka-listener
├── Dockerfile.streamlit
├── EDA_ML_Implementation/
├── README.md
├── a .env
├── airflow/
├── app/
│   └── app.py
├── archive/
├── dags/
│   └── data_pipeline_dag.py
├── data_crawled/
├── data_crawled_root/
├── data_json/
├── docker-compose.yml
├── hadoop.env
├── imageForProject/
├── json_output/
├── requirements-airflow.txt
├── requirements-base.txt
├── requirements-data.txt
├── requirements-scraping.txt
├── requirements-spark.txt
├── requirements-streaming.txt
├── requirements-streamlit.txt
├── requirements.txt
├── scripts/
│   ├── crawl.py
│   ├── convertTextToJson.py
│   ├── LoadDataIntoDataLake.py
│   ├── ETL.py
│   ├── ETL_transfer.py
│   ├── kafka_listeners.py
|   └── sendMail
└── setup.py
```
## Diagram:
  <div style="display: flex; justify-content: center; align-items: center; height: 100vh;">
      <img src="https://github.com/VietDucFCB/CarInsight-End-to-End-Data-Engineering-for-Used-Cars/blob/main/imageForProject/diagram.png" width="500"/>
  </div>
  
## Kiến Trúc Pipeline:

```
                   ┌────────────┐    ┌───────────────┐    ┌─────────────────────┐    ┌──────────┐    ┌────────────┐
                   │            │    │               │    │                     │    │          │    │            │
                   │ crawl.py   ├───►│ convertText   ├───►│ LoadDataIntoData    ├───►│  Kafka   ├───►│   ETL.py   │
                   │            │    │ ToJson.py     │    │ Lake.py             │    │          │    │            │
                   └────────────┘    └───────────────┘    └─────────────────────┘    └────┬─────┘    └────┬───────┘
                                                                          │                        │
                                                                          │                        │
                                                                          │                        ▼
                                                                          │                 ┌──────────────┐
                                                                          │                 │              │
                                                                          │                 │ Data         │
                                                                          │                 │ Warehouse    │
                                                                          │                 │              │
                                                                          │                 └──────┬───────┘
                                                                          │                        │
                                                                          │                        │
                                                                          ▼                        ▼
                                                                ┌─────────────┐         ┌──────────────┐
                                                                │             │         │              │
                                                                │   app.py    │◄────────┤    Kafka     │
                                                                │             │         │              │
                                                                └─────────────┘         └──────────────┘
```

Quy trình xử lý được mô tả chi tiết theo các task sau:

```
crawl_task >> convert_to_json_task >> load_to_datalake_task >> trigger_etl_kafka_task >> etl_task >> trigger_app_kafka_task >> send_email_task
```

Trong đó:
- **crawl_task**: Thu thập dữ liệu từ các nguồn qua `crawl.py`
- **convert_to_json_task**: Chuyển đổi dữ liệu thu thập được sang định dạng JSON với `convertTextToJson.py`
- **load_to_datalake_task**: Nạp dữ liệu vào Data Lake (ví dụ: HDFS) qua `LoadDataIntoDataLake.py`
- **trigger_etl_kafka_task**: Kích hoạt xử lý ETL qua Kafka
- **etl_task**: Thực hiện quá trình ETL trên dữ liệu với `ETL.py`
- **trigger_app_kafka_task**: Kích hoạt ứng dụng qua Kafka sau khi ETL hoàn thành
- **send_email_task**: Gửi email thông báo hoàn thành quy trình xử lý

Sơ đồ này minh họa rõ ràng hành trình của dữ liệu từ thu thập đến xử lý và phân phối kết quả trong hệ thống.

  <div style="display: flex; justify-content: center; align-items: center; height: 100vh;">
      <img src="https://github.com/VietDucFCB/CarInsight-End-to-End-Data-Engineering-for-Used-Cars/blob/main/imageForProject/pipeline_etl.png" width="900"/>
  </div>
  
## Các Thành Phần Chính

- **Thu Thập Dữ Liệu**: 
  - Script `crawl.py` được thiết lập để trích xuất dữ liệu từ các trang web khác nhau. Nó hỗ trợ thu thập dữ liệu tự động theo lịch trình đã định và có khả năng xử lý nhiều định dạng dữ liệu.
  
- **Biến Đổi Dữ Liệu**: 
  - Script `convertTextToJson.py` chuyển đổi dữ liệu thô từ các nguồn dạng văn bản thành định dạng JSON. 
  - Các script ETL khác chịu trách nhiệm làm sạch, định dạng lại dữ liệu và tạo điều kiện thuận lợi cho quá trình xử lý tiếp theo.

- **Lưu Trữ Dữ Liệu**: 
  - Hệ thống sử dụng HDFS để lưu trữ dữ liệu dưới dạng “data lake”, đảm bảo dữ liệu thô và đã qua xử lý được lưu giữ lâu dài và có thể truy xuất hiệu quả.

- **Xử Lý Dữ Liệu**: 
  - Apache Airflow được cấu hình để điều phối các quy trình làm việc phức tạp, từ việc chạy các pipeline của quá trình ETL đến việc thực thi các tác vụ xử lý dữ liệu theo lịch trình.

- **Hệ Thống Sự Kiện**: 
  - Kafka được triển khai để giám sát và kích hoạt các hành động dựa trên sự kiện dữ liệu. Điều này giúp hệ thống phản ứng nhanh với các thay đổi dữ liệu cũng như bất kỳ cảnh báo nào từ hệ thống.

- **Kho Dữ Liệu**: 
  - Dữ liệu có cấu trúc được nạp vào cơ sở dữ liệu PostgreSQL, giúp việc truy vấn và phân tích dữ liệu trở nên dễ dàng và nhanh chóng.

- **Ứng Dụng**: 
  - Một ứng dụng web được phát triển trên nền tảng Streamlit cung cấp giao diện trực quan để tư vấn mua xe, cho phép người dùng tương tác và ra quyết định dựa trên các phân tích dữ liệu.
  
## Quy Trình Xử Lý Dữ Liệu

1. **Thu Thập Dữ Liệu**: 
   - Crawler định kỳ chạy theo lịch trình để thu thập dữ liệu về xe ô tô cũ từ nhiều nguồn đa dạng. Quá trình này đảm bảo tất cả các nguồn dữ liệu liên quan đều được thu thập một cách đầy đủ.

2. **Lưu Trữ Thô**: 
   - Dữ liệu thu thập được ban đầu được lưu trữ dưới dạng JSON trong hệ thống tệp cục bộ, tạo thành kho chứa dữ liệu thô cho các bước xử lý tiếp theo.

3. **Nạp Vào Data Lake**: 
   - Sau khi thu thập, dữ liệu được chuyển từ hệ thống tệp cục bộ vào Data Lake trên HDFS, đảm bảo dung lượng lưu trữ lớn và khả năng truy xuất dữ liệu hiệu quả.

4. **ETL Cơ Bản**: 
   - Quá trình ETL (Extract, Transform, Load) cơ bản được thực hiện: dữ liệu thô được làm sạch, chuyển đổi, và nạp lại vào HDFS nhằm chuẩn bị cho quá trình xử lý chuyên sâu.

5. **Phân Vùng Dữ Liệu**: 
   - Dữ liệu trong HDFS được tổ chức lại theo cấu trúc phân vùng hợp lý, tối ưu hóa cho các tác vụ truy vấn và phân tích sau này.

6. **Xử Lý Nâng Cao**: 
   - Apache Spark được sử dụng để thực hiện các phân tích phức tạp trên dữ liệu; từ việc tính toán thống kê đến xây dựng mô hình dự đoán, Spark tạo ra các insights giá trị từ dữ liệu.

7. **Tải Vào Kho Dữ Liệu Chuyên Dụng**:
   - Các tập dữ liệu sau khi qua xử lý được chia tải vào các kho dữ liệu chuyên biệt:
     - Dữ liệu cho hệ thống tư vấn xe, giúp người dùng có được thông tin và phân tích chi tiết.
     - Dữ liệu cho hệ thống dự đoán bán hàng nhằm hỗ trợ đưa ra các kế hoạch kinh doanh chính xác.

8. **Ứng Dụng**:
   - **API**: Xây dựng API cho hệ thống tư vấn xe, cho phép truy vấn dữ liệu theo yêu cầu và đưa ra các phân tích theo thời gian thực.
   - **Công Cụ Phân Tích và Dự Đoán**: Triển khai các công cụ phân tích trực quan và dự đoán khả năng bán hàng, giúp ban quản lý có được cái nhìn tổng quan và ra quyết định chính xác.

9. **Phân Phối Quy Trình Làm Việc**:
   - Tất cả các bước quy trình xử lý dữ liệu được điều phối và tự động hóa thông qua Apache Airflow, giúp đảm bảo tính liên tục và hiệu quả trong toàn bộ hệ thống.

## Cách xây dựng
# Hướng Dẫn Cài Đặt và Sử Dụng Docker Cho Dự Án

## 1. Cài Đặt Docker Desktop

- Truy cập trang chủ của Docker: [Docker Desktop Download](https://www.docker.com/products/docker-desktop)
- Tải phiên bản Docker Desktop phù hợp với hệ điều hành của bạn (Windows hoặc Mac).
- Chạy file cài đặt và làm theo hướng dẫn trên màn hình để cài đặt Docker Desktop.
- Sau khi cài đặt xong, mở Docker Desktop để đảm bảo Docker đã chạy thành công. Bạn có thể kiểm tra bằng cách chạy lệnh sau trong terminal:
  
  ```bash
  docker --version
  ```

## 2. Cài Đặt Git

Nếu bạn chưa cài đặt Git, hãy tải từ trang chủ: [Git Downloads](https://git-scm.com/downloads) và làm theo hướng dẫn cài đặt tương ứng với hệ điều hành của bạn.

## 3. Clone Repository

Mở terminal hoặc Command Prompt, di chuyển đến thư mục mà bạn muốn lưu trữ dự án, sau đó clone repo từ GitHub bằng lệnh:

```bash
git clone https://github.com/VietDucFCB/CarInsight-End-to-End-Data-Engineering-for-Used-Cars.git
```

Vào thư mục dự án đã clone về:

```bash
cd CarInsight-End-to-End-Data-Engineering-for-Used-Cars
```

## 4. Build Docker Image

Trong thư mục chứa file `Dockerfile` (thường nằm ở root của dự án), bạn sẽ build image bằng lệnh sau. Bạn có thể thay `your-image-name` thành tên image mong muốn:

```bash
docker build -t your-image-name .
```

Lệnh này sẽ đọc file `Dockerfile` và build image dựa trên các chỉ dẫn bên trong.

## 5. Chạy Docker Container

Sau khi image được build thành công, bạn có thể chạy container bằng lệnh:

```bash
docker run -d -p 8080:80 --name your-container-name your-image-name
```

Trong đó:
- `-d` chạy container dưới dạng background.
- `-p 8080:80` ánh xạ cổng 80 bên trong container sang cổng 8080 trên host. Tùy chỉnh theo nhu cầu của bạn.
- `--name your-container-name` đặt tên cho container.
- `your-image-name` là tên image bạn đã build ở bước trước.

Bạn có thể kiểm tra các container đang chạy bằng lệnh:

```bash
docker ps
```

Để dừng container, sử dụng:

```bash
docker stop your-container-name
```

Để khởi động lại container, dùng:

```bash
docker start your-container-name
```

## 6. Kiểm Tra Ứng Dụng

Sau khi container đã chạy, mở trình duyệt và truy cập địa chỉ: [http://localhost:8080](http://localhost:8080) (hoặc cổng bạn đã cấu hình) để kiểm tra ứng dụng hoạt động.

---

Với các bước trên, bạn đã hoàn thành quá trình cài đặt Docker Desktop, clone repo và build cũng như chạy Docker container cho dự án.

# Sử Dụng Airflow Webserver để Quản Lý và Chạy DAGs Qua Web UI

Airflow Webserver cung cấp giao diện người dùng web cho phép bạn giám sát, quản lý và kích hoạt các DAGs. Dưới đây là các bước để khởi động Airflow Webserver và chạy DAGs thông qua giao diện web:

## 1. Cài Đặt Airflow

Nếu bạn chưa cài đặt Airflow, hãy cài đặt qua pip hoặc Docker tùy thuộc vào môi trường làm việc của bạn. Ví dụ, cài đặt Airflow qua pip:
```bash
pip install apache-airflow
```

## 2. Thiết Lập Airflow

- **Đặt thư mục Airflow**: Tạo thư mục cho Airflow và thiết lập biến môi trường `AIRFLOW_HOME`. Ví dụ:
```bash
mkdir ~/airflow
export AIRFLOW_HOME=~/airflow
```
- **Khởi tạo cơ sở dữ liệu**: Airflow dùng SQLite (trong môi trường phát triển) nên thực hiện lệnh sau để khởi tạo cơ sở dữ liệu:
```bash
airflow db init
```
- **Tạo người dùng Admin**: Tạo người dùng để đăng nhập vào giao diện Airflow:
```bash
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com
```

## 3. Khởi Động Scheduler và Webserver

### Khởi Động Scheduler

Scheduler chịu trách nhiệm kích hoạt các tác vụ theo lịch trình của DAG. Mở một terminal và chạy:
```bash
airflow scheduler
```

### Khởi Động Webserver

Mở một terminal khác để khởi động Airflow Webserver:
```bash
airflow webserver --port 8080
```
Sau khi khởi động thành công, bạn có thể truy cập giao diện Airflow tại:
[http://localhost:8080](http://localhost:8080)

## 4. Tải DAGs Vào Airflow

- **Đặt DAGs**: Đảm bảo rằng các file DAG (ví dụ: `data_pipeline_dag.py` nằm trong thư mục `dags/`) được đặt trong thư mục DAG mà Airflow sử dụng. Mặc định, Airflow sẽ tìm các DAG trong thư mục `${AIRFLOW_HOME}/dags`.
- Nếu bạn muốn thay đổi thư mục DAG, hãy thiết lập biến môi trường:
```bash
export AIRFLOW__CORE__DAGS_FOLDER=~/path/to/your/dags
```
- Sau khi DAGs được đặt đúng vị trí, Airflow Webserver sẽ tự động phát hiện và nạp các file DAG từ thư mục này.

## 5. Sử Dụng Web UI để Quản Lý và Chạy DAG

- Mở trình duyệt và truy cập [http://localhost:8080](http://localhost:8080) để vào giao diện Airflow. (Trong cấu hình của tôi là 8081)
- **Quan sát danh sách DAGs**: Tại trang chính, bạn sẽ thấy danh sách các DAG hiện có.
- **Xem chi tiết DAG**: Nhấp vào tên của một DAG để xem thông tin chi tiết, biểu đồ và biểu đồ trạng thái của các task.
- **Kích hoạt DAG**:
  - Để chạy DAG ngay lập tức, nhấp vào nút "Trigger DAG" (thường hiển thị dưới dạng biểu tượng "play") bên cạnh tên DAG.
  - Bạn cũng có thể bật/tắt chế độ chạy theo lịch bằng cách sử dụng nút "On/Off" trong giao diện.
- **Xem logs và trạng thái**: Sử dụng các tab chi tiết bên trong để theo dõi trạng thái hiện tại của các task và kiểm tra logs của các lần chạy trước đó.

## 6. Dừng Airflow

- Để dừng Airflow Webserver, nhấn `CTRL+C` trong terminal mà bạn đã khởi động webserver.
- Tương tự, dừng scheduler bằng cách nhấn `CTRL+C` trong terminal nơi scheduler đang chạy.

  <div style="display: flex; justify-content: center; align-items: center; height: 100vh;">
      <img src="https://github.com/VietDucFCB/CarInsight-End-to-End-Data-Engineering-for-Used-Cars/blob/main/imageForProject/airflow.png" width="900"/>
  </div>
  
1. Nhấp vào biểu tượng nếu bạn muốn khởi động lại hoặc bạn đã có cập nhật trong code trước đó
2. Xem DAGS Directed Acyclic Graphs (Đồ thị có hướng không chu trình).
3. Xem đồ thị các task trong ETL
4. Chạy hoặc xóa tiến trình

Với các bước trên, bạn có thể dễ dàng khởi động Airflow Webserver, tải DAGs vào hệ thống và sử dụng giao diện web để quản lý cũng như chạy các quy trình xử lý dữ liệu của bạn.

### Tự động gửi Email khi chạy ETL:
- Kể cả DAGS có chạy thành công hay thất bại cũng sẽ gửi về mail thông báo cho người nhận:
  <div style="display: flex; justify-content: center; align-items: center; height: 100vh;">
      <img src="https://github.com/VietDucFCB/CarInsight-End-to-End-Data-Engineering-for-Used-Cars/blob/main/imageForProject/Email.png" width="900"/>
  </div>

- Bạn có thể thay đổi email người nhận bằng cách cấu hình file `docker-compose.yml` như sau:
```
  environments:
    AIRFLOW__WEBSERVER__BASE_URL: 'http://localhost:8081'
    # SMTP Configuration for Email Delivery
    SMTP_HOST: smtp-mail.outlook.com
    SMTP_PORT: 587
    SMTP_USER: USER_SEND_MAIL
    SMTP_PASSWORD: PASSWORD_SEND_MAIL
    SMTP_USE_TLS: 'False'
    FROM_EMAIL: USER_RECEIVE_MAIL
    EMAIL_DELIVERY_METHOD: smtp
    # Rest of your code
```
## Hệ thống tư vấn gợi ý mua xe theo yêu cầu của khách hàng:
Người dùng thông qua các thông tin sau: Năm sản xuất, nhà sản xuất xe mong muốn, Giá trong một phạm vi nhất định, có chính sách trả góp hay không, v.v ... Loại động cơ nào, sử dụng nhiên liệu nào và một số đặc điểm nếu cần thiết. Ứng dụng sẽ truy vấn cơ sở dữ liệu có sẵn trong PostgreSQL, thông tin được nhập bởi người dùng có thể trống, sau đó đầu ra sẽ là tất cả thông tin của xe theo yêu cầu của nhà nhập khẩu và được sắp xếp bằng cách tăng giá.

  <div style="display: flex; justify-content: center; align-items: center; height: 100vh;">
      <img src="https://github.com/VietDucFCB/ProjectSummer2024/blob/main/2.png" width="900"/>
  </div>
  
[Used car recomendation system](https://carinsight-end-to-end-data-engineering-for-used-cars-myh5xntg3.streamlit.app/)

## Mô hình Machine Learning dự doán khi nào xe có thể được bán trong tương lai:

  <div style="display: flex; justify-content: center; align-items: center; height: 100vh;">
      <img src="https://github.com/VietDucFCB/ProjectSummer2024/blob/main/9.png" width="500"/>
  </div>
