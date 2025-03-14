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
├── .github/
│   └── workflows/
│       └── pipeline.yml
├── dags/
│   └── data_pipeline_dag.py
├── scripts/
│   ├── crawl.py
│   ├── convertTextToJson.py
│   ├── LoadDataIntoDataLake.py
│   ├── ETL.py
│   ├── ETL_transfer.py
│   └── kafka_listeners.py
├── app/
│   └── app.py
├── requirements.txt
├── setup.py
└── README.md
```
## Diagram:
  <div style="display: flex; justify-content: center; align-items: center; height: 100vh;">
      <img src="https://github.com/VietDucFCB/CarInsight-End-to-End-Data-Engineering-for-Used-Cars/blob/main/Diagram.png" width="500"/>
  </div>
  
## Kiến Trúc Pipeline:
```
┌────────────┐    ┌────────────┐    ┌──────────────────┐    ┌────────┐    ┌────────────┐
│            │    │            │    │                  │    │        │    │            │
│  crawl.py  ├───►│convertText ├───►│LoadDataIntoData  ├───►│  Kafka ├───►│   ETL.py   │
│            │    │ToJson.py   │    │Lake.py           │    │        │    │            │
└────────────┘    └────────────┘    └──────────────────┘    └────┬───┘    └─────┬──────┘
                                                                 │              │
                                                                 │              │
                                                                 │              ▼
                                                                 │        ┌──────────────┐
                                                                 │        │              │
                                                                 │        │ETL_transfer.py│
                                                                 │        │              │
                                                                 │        └──────┬───────┘
                                                                 │               │
                                                                 │               │
                                                                 ▼               ▼
                                                           ┌─────────┐    ┌──────────────┐
                                                           │         │    │              │
                                                           │  app.py │◄───┤Data Warehouse│
                                                           │         │    │              │
                                                           └─────────┘    └──────────────┘
```

## Các Thành Phần Chính

- **Thu Thập Dữ Liệu**: `crawl.py` trích xuất dữ liệu từ các trang web
- **Biến Đổi Dữ Liệu**: `convertTextToJson.py` và các script ETL xử lý dữ liệu thô
- **Lưu Trữ Dữ Liệu**: HDFS làm giải pháp data lake
- **Xử Lý Dữ Liệu**: Apache Airflow để điều phối quy trình làm việc
- **Hệ Thống Sự Kiện**: Kafka để kích hoạt các hành động dựa trên sự kiện dữ liệu
- **Kho Dữ Liệu**: Cơ sở dữ liệu PostgreSQL để lưu trữ dữ liệu có cấu trúc
- **Ứng Dụng**: Ứng dụng web dựa trên Streamlit để đưa ra hệ thống tư vấn mua xe
- **CI/CD**: GitHub Actions để tự động hóa thực thi pipeline

## Quy Trình Xử Lý Dữ Liệu
1. Thu Thập Dữ Liệu: Crawler định kỳ thu thập dữ liệu về xe ô tô cũ từ nhiều nguồn khác nhau
2. Lưu Trữ Thô: Dữ liệu được lưu trữ dưới dạng JSON trong hệ thống tệp cục bộ
3. Nạp Vào Data Lake: Dữ liệu được chuyển vào Data Lake HDFS để lưu trữ lâu dài
4. ETL Cơ Bản: Dữ liệu được làm sạch, chuyển đổi và nạp vào HDFS
5. Phân Vùng Dữ Liệu: Dữ liệu trong HDFS được tổ chức theo cấu trúc phân vùng hiệu quả
6. Xử Lý Nâng Cao: Apache Spark thực hiện các phân tích phức tạp trên dữ liệu
7. Tải Vào Kho Dữ Liệu Chuyên Dụng:
  - Dữ liệu cho hệ thống tư vấn xe
  - Dữ liệu cho hệ thống dự đoán bán hàng
8. Ứng Dụng:
- API cho hệ thống tư vấn xe theo yêu cầu
- Công cụ phân tích và dự đoán khả năng bán hàng

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
