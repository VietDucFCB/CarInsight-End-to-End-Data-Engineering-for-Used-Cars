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
## Diagram:
  <div style="display: flex; justify-content: center; align-items: center; height: 100vh;">
      <img src="https://github.com/VietDucFCB/CarInsight-End-to-End-Data-Engineering-for-Used-Cars/blob/main/Diagram.png" width="500"/>
  </div>
  
## Kiến Trúc Pipeline:



## Các Thành Phần Chính

- **Thu Thập Dữ Liệu**: `crawl.py` trích xuất dữ liệu từ các trang web
- **Biến Đổi Dữ Liệu**: `convertTextToJson.py` và các script ETL xử lý dữ liệu thô
- **Lưu Trữ Dữ Liệu**: MinIO làm giải pháp data lake
- **Xử Lý Dữ Liệu**: Apache Airflow để điều phối quy trình làm việc
- **Hệ Thống Sự Kiện**: Kafka để kích hoạt các hành động dựa trên sự kiện dữ liệu
- **Kho Dữ Liệu**: Cơ sở dữ liệu PostgreSQL để lưu trữ dữ liệu có cấu trúc
- **Ứng Dụng**: Ứng dụng web dựa trên Streamlit để đưa ra hệ thống tư vấn mua xe
- **CI/CD**: GitHub Actions để tự động hóa thực thi pipeline

## Cài Đặt

1. Clone repository:
   ```bash
   git clone https://github.com/VietDucFCB/CarInsight-End-to-End-Data-Engineering-for-Used-Cars.git
   cd CarInsight-End-to-End-Data-Engineering-for-Used-Cars```
