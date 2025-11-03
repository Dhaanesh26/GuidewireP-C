import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import psycopg2


# 1. Spark Session Setup

def get_spark():
    return SparkSession.builder \
        .appName("PySparkAivenPostgreSQL") \
        .config("spark.jars", "/Users/siddharth/Desktop/postgresql-42.7.3.jar") \
        .getOrCreate()


# 2. Postgres Connection Setup

def get_pg_connection():
    aiven_host = "pg-706f91-dhaanesh-377f.l.aivencloud.com"
    aiven_port = 21510
    aiven_db = "defaultdb"
    aiven_user = "avnadmin"
    aiven_password = "AVNS_91ooJLE6UByU8h5QoYB"
    ssl_mode = "require"

    conn = psycopg2.connect(
        host=aiven_host,
        port=aiven_port,
        dbname=aiven_db,
        user=aiven_user,
        password=aiven_password,
        sslmode=ssl_mode
    )
    conn.autocommit = True
    return conn


# 3. Ensure Tables Exist (lowercase schema)

def ensure_tables(conn):
    cur = conn.cursor()

    # Drop and recreate inbound_file_data
    cur.execute("""
        DROP TABLE IF EXISTS inbound_file_data;
        CREATE TABLE inbound_file_data (
            customer_id INT PRIMARY KEY,
            customer_name VARCHAR(100),
            customer_email VARCHAR(150),
            amount DECIMAL(10,2),
            orig_source_file VARCHAR(255),
            updt_source_file VARCHAR(255),
            created_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    # Drop and recreate staging_inbound
    cur.execute("""
        DROP TABLE IF EXISTS staging_inbound;
        CREATE TABLE staging_inbound (
            customer_id INT,
            customer_name VARCHAR(100),
            customer_email VARCHAR(150),
            amount DECIMAL(10,2),
            orig_source_file VARCHAR(255),
            updt_source_file VARCHAR(255)
        );
    """)

    cur.close()
    print("Ensured inbound_file_data and staging_inbound tables exist (lowercase)")


# 4. Main ETL Logic

def main():
    spark = get_spark()
    conn = get_pg_connection()

    ensure_tables(conn)

    # --- Step 1: Load CSVs ---
    csv_files = [
        "/Users/siddharth/Exavalu/Learning/Tasks/Src_File_1.csv",
        "/Users/siddharth/Exavalu/Learning/Tasks/Src_File_2.csv",
        "/Users/siddharth/Exavalu/Learning/Tasks/Src_File_3.csv"
    ]

    dfs = []
    for path in csv_files:
        print(f"Loading {os.path.basename(path)} ...")
        df = spark.read.csv(path, header=True, inferSchema=True)
        df = df.withColumnRenamed("Customer_ID", "customer_id") \
               .withColumnRenamed("Customer_Name", "customer_name") \
               .withColumnRenamed("Customer_Email", "customer_email") \
               .withColumnRenamed("Amount", "amount") \
               .withColumn("orig_source_file", lit(os.path.basename(path))) \
               .withColumn("updt_source_file", lit(os.path.basename(path)))
        dfs.append(df)

    combined_df = dfs[0].unionByName(dfs[1]).unionByName(dfs[2])
    print("Combined inbound data:")
    combined_df.show(5)

    # --- Step 2: Write to staging_inbound ---
    pg_url = "jdbc:postgresql://pg-706f91-dhaanesh-377f.l.aivencloud.com:21510/defaultdb?sslmode=require"
    pg_properties = {
        "user": "avnadmin",
        "password": "AVNS_91ooJLE6UByU8h5QoYB",
        "driver": "org.postgresql.Driver"
    }

    print("Writing data into staging table: staging_inbound")
    combined_df.write.jdbc(url=pg_url, table="staging_inbound", mode="overwrite", properties=pg_properties)

    # --- Step 3: Run UPSERT ---
    upsert_sql = """
        INSERT INTO inbound_file_data (customer_id, customer_name, customer_email, amount, orig_source_file, updt_source_file)
        SELECT customer_id, customer_name, customer_email, amount, orig_source_file, updt_source_file
        FROM staging_inbound
        ON CONFLICT (customer_id)
        DO UPDATE SET
            customer_name = EXCLUDED.customer_name,
            customer_email = EXCLUDED.customer_email,
            amount = EXCLUDED.amount,
            updt_source_file = EXCLUDED.updt_source_file,
            updated_on = CURRENT_TIMESTAMP;
    """
    cur = conn.cursor()
    print("Running UPSERT into inbound_file_data ...")
    cur.execute(upsert_sql)
    cur.close()

    # --- Step 4: Verify counts ---
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM inbound_file_data;")
    count = cur.fetchone()[0]
    cur.close()
    print(f"Final record count in inbound_file_data: {count}")

    conn.close()
    spark.stop()
    print("▶️ Job finished successfully!")

# Entry point

if __name__ == "__main__":
    main()
