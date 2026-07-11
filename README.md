# CateLytics: Review-Driven Categorization using Amazon Data

## Project Overview
**CateLytics** is a Big Data project designed to categorize Amazon products based on insights derived from user reviews. The goal is to identify trends around customer-centric product categories by analyzing user reviews. This approach uncovers hidden product categories that may not align with conventional groupings, providing a more intuitive understanding of products through user feedback.

The project leverages **Spark** to perform data operations such as Exploratory Data Analysis (EDA), data preprocessing, and transformations. By processing and analyzing large amounts of unstructured data, it helps customers find suitable products based on real user feedback and sentiment, rather than traditional product descriptions or ratings. Additionally, CateLytics aims to assist customers in making informed and ethical purchasing decisions using natural language queries through an integrated Chatbot.

## Project Structure

The code repository is organized into the following folders, each representing different phases and components of the project:

### 1. **src**
- Reserved for shared data-processing utilities.

### 2. **EDA**
- Contains all the files related to Data Exploration and Data Understanding.

### 3. **MISC**
- Includes miscellaneous code for sample creation, testing, validation, and access checks.

### 4. **Preprocessing**
- Contains code related to data cleaning, data transformation, and data preparation.

### 5. **Vector Database**
- Includes code and files related to the Vector Database setup in EC2 instance, processing, and data ingestion into **Weaviate**.

### 6. **chatbot**
- Contains code related to RAG & LLM integration for building the Chatbot.

### 7. **scripts**
- Shell scripts for provisioning/tearing down EC2 instances and running the EMR unzip job.

## How to Use This Repository
- Follow the documentation and examples provided in each folder to set up and run each phase of the project.
- Install dependencies from `requirements.txt` at the repo root before running any component.

## Key Features
- **Sentiment Analysis:** Extracts and analyzes user sentiments from reviews.
- **Natural Language Processing:** Employs a chatbot to allow users to search and find product information based on reviews.
- **Data Scalability:** Utilizes Spark and MapReduce for handling large volumes of data.
- **Intelligent Recommendations:** Aims to provide more informed, review-based product summarisation.

## Getting Started
1. Clone this repository to your local machine.
2. Install dependencies: `pip install -r requirements.txt`.
3. Navigate to the `EDA` folder for initial data analysis and insights.
4. Move on to the `Preprocessing` and `Vector Database` folders for data preparation and setup.
5. Integrate the `chatbot` component for a more interactive user experience.

## Running the Chatbot
1. Start Weaviate: `cd "Vector Database" && docker compose up -d weaviate`.
2. Set `WEAVIATE_URL` (defaults to `http://localhost:8080` if unset) and ingest data with `Vector Database/Vector_Data_Ingestion_into_Weaviate.py`.
3. Place your fine-tuned T5 summarizer weights (e.g. `pytorch_model.bin` / `model.safetensors`, produced by `chatbot/Fine_Tune.ipynb`) in `chatbot/models/fine_tuned_t5/` — only the tokenizer files are checked in, so the summarizer cannot load without them.
4. Run `cd chatbot && python main.py`.

---

For any questions or additional information, please contact the project team.
