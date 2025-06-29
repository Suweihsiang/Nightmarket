# Nightmarket 水軍評論分析專案

本專案為緯育資料工程師養成班第六期第一組之專案作品，旨在探討**夜市水軍評論之分布情形**。本 repository 主要著重於 ETL 部分，包含資料蒐集、清洗、儲存與每日排程爬蟲等功能。

---

## 專案內容簡介

本專案選定九家夜市，並透過 `Selenium` 套件於 Google Maps 爬取夜市中的店家名單。為避免蒐集到非夜市範圍的店家，透過計算店家經緯度與夜市中心的距離，篩選出實際位於夜市範圍內的店家。

在獲得店家名單後，進一步爬取各店家於 Google Maps 上的評論，並選擇**最新排序**以盡可能收集到完整評論資料。評論內容包含：

- 店家地址
- 熱門留言標籤
- 評論者 ID
- 在地嚮導標籤
- 發布之評論數與相片數
- 評論星等與文字內容

評論資料經過清洗後，會儲存至 MySQL 資料庫中。

---

## 自動化排程與最佳化設計

透過 **Airflow DAG**，每日排程自動爬取各店家當日最新留言。每家店家評論爬取被設計成一個 Task 並放入 Task Group：

- 每次最多**平行爬取三間店家**，大幅減少總爬取時間（從 9 小時縮短至 3 小時）
- 使用 Retry 機制，當某店家評論爬取失敗時可自動重試
- 爬取結束後，進行資料清洗，並將結果儲存進 MySQL

---

## 開發與執行環境

本專案使用以下工具與設定，並以Docker為主要環境建構方式：

- **作業系統**：Ubuntu 22.04（透過DevContainer運行）
- **Python 版本**：Python 3.12
- **套件管理工具**：Poetry（用於Python套件安裝與虛擬環境管理）
- **資料庫**：
  - MySQL 8.0（Docker 映像檔：`mysql:8.0`）
  - phpMyAdmin（MySQL的Web UI介面，Docker 映像檔：`phpmyadmin/phpmyadmin`）
- **排程工具**：
  - Apache Airflow 2.10.5（Docker 映像檔：`apache/airflow:2.10.5-python3.12`）
  - PostgreSQL 13（Airflow metadata 資料庫，Docker 映像檔：`postgres:13`）
- **ETL 工具與套件**：
  - `selenium`（自動化爬蟲）
  - `pandas`、`re`（資料清洗）
- **容器與網路設定**：
  - Docker + Docker Compose
  - 自定義網路：`nightmarket`（用於多容器通訊）

---

