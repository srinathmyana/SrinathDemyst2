
# **Airflow-Based CSV Anonymization**

This project provides a solution to anonymize large CSV files using Apache Airflow. It processes the CSV in chunks to anonymize `first_name`, `last_name`, and `address`, ensuring scalability for large datasets.

---

## **1. Prerequisites**
- **Docker**: For containerization.
- **Python 3.x**: For standalone testing.
- **Required Python Libraries**: `pandas`.

---

## **2. Directory Structure**
Demyst2/
- airflow/dags/anonymize_dag.py: Airflow DAG for anonymization.
- process_anonymize_csv.py: Script to anonymize CSV data.
- large_input.csv: Sample input file.
- output/: Directory for anonymized output files.
- Dockerfile: For containerization.
- docker-compose.yaml: For deploying Airflow.

---

## **3. Instructions**
1. **Setup Docker**:
   - Build: `docker build -t airflow-csv-anonymizer:latest .`
   - Start: `docker-compose up -d`
   - Access Airflow at http://localhost:8080.

2. **Run the DAG**:
   - Copy `anonymize_dag.py` to Airflow’s `dags/` folder.
   - Restart Airflow: `docker restart airflow-1`.
   - Trigger the DAG `anonymize_large_csv` in the Airflow UI.

3. **Output**:
   - Anonymized file will be saved in `output/anonymized_large_output.csv`.

---

## **4. Notes**
- Supports chunk processing for large datasets.
- Paths must align with your setup or containerized environment.

--- 

Let me know if you need further edits!