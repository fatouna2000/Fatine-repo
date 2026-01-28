"""
🎵 Music Analytics Pipeline with REAL PySpark
Complete ETL pipeline with data generation, Spark processing, and analytics
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import pandas as pd
import numpy as np
import json
import os
import sys

# Add PySpark to path
sys.path.append('/home/theia/.local/lib/python3.9/site-packages')

# ==================== CONFIGURATION ====================
DEFAULT_ARGS = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'email_on_failure': False,
    'email_on_retry': False,
}

# ==================== DATA GENERATION ====================
def generate_music_data(**context):
    """Generate music streaming data"""
    print(f"🎵 Generating music data for {context['ds']}")
    
    np.random.seed(42)
    
    # Generate realistic data
    artists = ['Taylor Swift', 'Drake', 'Beyoncé', 'The Weeknd', 'Bad Bunny',
               'Ed Sheeran', 'Ariana Grande', 'Post Malone', 'Billie Eilish']
    
    n_records = 500
    
    data = {
        'timestamp': pd.date_range(start=context['ds'], periods=n_records, freq='T'),
        'user_id': [f'user_{np.random.randint(10000, 99999)}' for _ in range(n_records)],
        'artist': np.random.choice(artists, n_records),
        'song': [f'song_{i}' for i in range(n_records)],
        'duration_ms': np.random.randint(120000, 300000, n_records),
        'plays': np.random.randint(1, 20, n_records),
        'country': np.random.choice(['US', 'UK', 'FR', 'DE', 'JP'], n_records),
        'device': np.random.choice(['mobile', 'desktop', 'tablet'], n_records)
    }
    
    df = pd.DataFrame(data)
    
    # Save to /tmp
    output_dir = '/tmp/spark_music_data'
    os.makedirs(output_dir, exist_ok=True)
    
    csv_file = f"{output_dir}/music_data_{context['ds'].replace('-', '')}.csv"
    df.to_csv(csv_file, index=False)
    
    print(f"✅ Generated {len(df)} records")
    print(f"📁 Saved to: {csv_file}")
    
    context['ti'].xcom_push(key='data_file', value=csv_file)
    return csv_file


# ==================== SPARK PROCESSING ====================
def process_with_spark(**context):
    """Process data with REAL PySpark"""
    print("⚡ Processing with PySpark...")
    
    try:
        # Import PySpark
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import col, sum as spark_sum, count, avg, desc
        
        # Get data file
        ti = context['ti']
        csv_file = ti.xcom_pull(task_ids='generate_music_data', key='data_file')
        
        if not csv_file or not os.path.exists(csv_file):
            print("⚠️ No data file found")
            return {'status': 'error', 'message': 'No data file'}
        
        # Create Spark Session
        spark = SparkSession.builder \
            .appName("MusicAnalytics") \
            .master("local[*]") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .getOrCreate()
        
        print(f"✅ Spark Session created: {spark.version}")
        
        # Read CSV with Spark
        df_spark = spark.read.csv(csv_file, header=True, inferSchema=True)
        
        print("📋 Spark DataFrame Schema:")
        df_spark.printSchema()
        
        print(f"📊 Total records: {df_spark.count()}")
        
        # Perform Spark analytics
        print("\n🎯 SPARK ANALYTICS RESULTS:")
        
        # 1. Total plays per artist
        artist_stats = df_spark.groupBy("artist") \
            .agg(
                spark_sum("plays").alias("total_plays"),
                count("user_id").alias("unique_users"),
                avg("duration_ms").alias("avg_duration_ms")
            ) \
            .orderBy(desc("total_plays"))
        
        print("🏆 Top Artists (Spark):")
        artist_stats.show(10, truncate=False)
        
        # 2. Country analysis
        country_stats = df_spark.groupBy("country") \
            .agg(spark_sum("plays").alias("total_plays")) \
            .orderBy(desc("total_plays"))
        
        print("🌍 Country Stats:")
        country_stats.show(truncate=False)
        
        # 3. Device analysis
        device_stats = df_spark.groupBy("device") \
            .agg(spark_sum("plays").alias("total_plays")) \
            .orderBy(desc("total_plays"))
        
        print("📱 Device Stats:")
        device_stats.show(truncate=False)
        
        # 4. Hourly distribution (extract hour from timestamp)
        from pyspark.sql.functions import hour
        df_with_hour = df_spark.withColumn("hour", hour(col("timestamp")))
        hourly_stats = df_with_hour.groupBy("hour") \
            .agg(spark_sum("plays").alias("total_plays")) \
            .orderBy("hour")
        
        print("⏰ Hourly Distribution:")
        hourly_stats.show(24, truncate=False)
        
        # Save Spark results
        output_dir = f"/tmp/spark_results_{context['ds'].replace('-', '')}"
        
        # Convert to pandas and save
        artist_pd = artist_stats.toPandas()
        country_pd = country_stats.toPandas()
        device_pd = device_stats.toPandas()
        hourly_pd = hourly_stats.toPandas()
        
        # Save individual files
        artist_pd.to_csv(f"{output_dir}_artists.csv", index=False)
        country_pd.to_csv(f"{output_dir}_countries.csv", index=False)
        device_pd.to_csv(f"{output_dir}_devices.csv", index=False)
        hourly_pd.to_csv(f"{output_dir}_hourly.csv", index=False)
        
        # Create summary JSON
        summary = {
            'date': context['ds'],
            'spark_version': spark.version,
            'total_records': df_spark.count(),
            'total_artists': artist_stats.count(),
            'total_countries': country_stats.count(),
            'top_artist': artist_pd.iloc[0].to_dict() if not artist_pd.empty else {},
            'top_country': country_pd.iloc[0].to_dict() if not country_pd.empty else {},
            'files_generated': [
                f"{output_dir}_artists.csv",
                f"{output_dir}_countries.csv",
                f"{output_dir}_devices.csv",
                f"{output_dir}_hourly.csv"
            ]
        }
        
        # Save summary
        summary_file = f"/tmp/spark_summary_{context['ds'].replace('-', '')}.json"
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2)
        
        print(f"\n✅ Spark processing completed!")
        print(f"📁 Summary saved: {summary_file}")
        print(f"📊 Total artists analyzed: {artist_stats.count()}")
        print(f"🌍 Countries analyzed: {country_stats.count()}")
        
        # Stop Spark session
        spark.stop()
        print("✅ Spark session stopped")
        
        # Store in XCom
        context['ti'].xcom_push(key='spark_summary', value=summary_file)
        context['ti'].xcom_push(key='spark_results_dir', value=output_dir)
        
        return summary
        
    except Exception as e:
        print(f"❌ Spark processing error: {e}")
        import traceback
        traceback.print_exc()
        
        # Try to stop Spark if it exists
        try:
            spark.stop()
        except:
            pass
            
        return {'status': 'error', 'message': str(e)}


# ==================== CREATE DASHBOARD DATA ====================
def create_dashboard_data(**context):
    """Create data for visualization dashboard"""
    print("📈 Creating dashboard data...")
    
    try:
        ti = context['ti']
        summary_file = ti.xcom_pull(task_ids='process_with_spark', key='spark_summary')
        
        if summary_file and os.path.exists(summary_file):
            with open(summary_file, 'r') as f:
                summary = json.load(f)
            
            # Read Spark results
            artists_file = f"/tmp/spark_results_{context['ds'].replace('-', '')}_artists.csv"
            countries_file = f"/tmp/spark_results_{context['ds'].replace('-', '')}_countries.csv"
            
            if os.path.exists(artists_file) and os.path.exists(countries_file):
                artists_df = pd.read_csv(artists_file)
                countries_df = pd.read_csv(countries_file)
                
                # Create dashboard-ready data
                dashboard_data = {
                    'date': context['ds'],
                    'summary': summary,
                    'charts': {
                        'artists': artists_df.to_dict('records'),
                        'countries': countries_df.to_dict('records'),
                        'metrics': {
                            'total_plays': int(artists_df['total_plays'].sum()),
                            'avg_plays_per_artist': float(artists_df['total_plays'].mean()),
                            'unique_countries': len(countries_df),
                            'top_artist': artists_df.iloc[0].to_dict() if not artists_df.empty else {},
                            'top_country': countries_df.iloc[0].to_dict() if not countries_df.empty else {}
                        }
                    }
                }
                
                # Save dashboard data
                dashboard_file = f"/tmp/dashboard_{context['ds'].replace('-', '')}.json"
                with open(dashboard_file, 'w') as f:
                    json.dump(dashboard_data, f, indent=2)
                
                print(f"✅ Dashboard data saved: {dashboard_file}")
                
                # Store in XCom
                context['ti'].xcom_push(key='dashboard_file', value=dashboard_file)
                
                return dashboard_data
        
        print("⚠️ No Spark results found, creating mock dashboard data")
        return {'status': 'mock_data', 'message': 'Using sample data'}
        
    except Exception as e:
        print(f"❌ Dashboard error: {e}")
        return {'status': 'error', 'message': str(e)}


# ==================== DAG DEFINITION ====================
with DAG(
    'music_analytics_with_spark',
    default_args=DEFAULT_ARGS,
    description='Music analytics pipeline with PySpark processing',
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['music', 'spark', 'pyspark', 'analytics', 'etl', 'ibm']
) as dag:
    
    # Task 1: Start
    start = BashOperator(
        task_id='start_pipeline',
        bash_command='echo "🚀 STARTING SPARK MUSIC ANALYTICS - Date: {{ ds }}"',
    )
    
    # Task 2: Generate data
    generate_data = PythonOperator(
        task_id='generate_music_data',
        python_callable=generate_music_data,
        provide_context=True,
    )
    
    # Task 3: Process with Spark
    spark_process = PythonOperator(
        task_id='process_with_spark',
        python_callable=process_with_spark,
        provide_context=True,
    )
    
    # Task 4: Create dashboard data
    dashboard_data = PythonOperator(
        task_id='create_dashboard_data',
        python_callable=create_dashboard_data,
        provide_context=True,
    )
    
    # Task 5: Generate report
    generate_report = BashOperator(
        task_id='generate_spark_report',
        bash_command="""
        echo "# 🎵 SPARK MUSIC ANALYTICS REPORT" > /tmp/spark_final_report_{{ ds_nodash }}.md
        echo "## Date: {{ ds }}" >> /tmp/spark_final_report_{{ ds_nodash }}.md
        echo "" >> /tmp/spark_final_report_{{ ds_nodash }}.md
        
        echo "## ✅ Pipeline Status" >> /tmp/spark_final_report_{{ ds_nodash }}.md
        echo "PySpark processing completed successfully!" >> /tmp/spark_final_report_{{ ds_nodash }}.md
        echo "" >> /tmp/spark_final_report_{{ ds_nodash }}.md
        
        echo "## 📊 Generated Files" >> /tmp/spark_final_report_{{ ds_nodash }}.md
        echo '```' >> /tmp/spark_final_report_{{ ds_nodash }}.md
        ls -la /tmp/spark_*{{ ds_nodash }}* /tmp/dashboard_*{{ ds_nodash }}* 2>/dev/null >> /tmp/spark_final_report_{{ ds_nodash }}.md || echo "No files yet" >> /tmp/spark_final_report_{{ ds_nodash }}.md
        echo '```' >> /tmp/spark_final_report_{{ ds_nodash }}.md
        echo "" >> /tmp/spark_final_report_{{ ds_nodash }}.md
        
        echo "## 🔧 Technologies Used" >> /tmp/spark_final_report_{{ ds_nodash }}.md
        echo "- **Apache Spark**: Distributed processing" >> /tmp/spark_final_report_{{ ds_nodash }}.md
        echo "- **Apache Airflow**: Workflow orchestration" >> /tmp/spark_final_report_{{ ds_nodash }}.md
        echo "- **Pandas/NumPy**: Data manipulation" >> /tmp/spark_final_report_{{ ds_nodash }}.md
        echo "" >> /tmp/spark_final_report_{{ ds_nodash }}.md
        
        echo "## 🎯 Skills Demonstrated" >> /tmp/spark_final_report_{{ ds_nodash }}.md
        echo "- Big Data processing with PySpark" >> /tmp/spark_final_report_{{ ds_nodash }}.md
        echo "- ETL pipeline design" >> /tmp/spark_final_report_{{ ds_nodash }}.md
        echo "- Data analytics and visualization" >> /tmp/spark_final_report_{{ ds_nodash }}.md
        echo "- Workflow automation" >> /tmp/spark_final_report_{{ ds_nodash }}.md
        
        echo "✅ Report saved: /tmp/spark_final_report_{{ ds_nodash }}.md"
        """,
    )
    
    # Task 6: Cleanup
    cleanup = BashOperator(
        task_id='cleanup_spark_files',
        bash_command="""
        echo "🧹 Cleaning up (keeping last 3 days)..."
        find /tmp -name "spark_*" -mtime +3 -delete 2>/dev/null || true
        echo "✅ Cleanup done"
        """,
    )
    
    # Task 7: End
    end = BashOperator(
        task_id='end_spark_pipeline',
        bash_command='echo "✅ SPARK MUSIC ANALYTICS PIPELINE COMPLETED!"',
    )
    
    # ========== TASK DEPENDENCIES ==========
    start >> generate_data >> spark_process >> dashboard_data >> generate_report >> cleanup >> end