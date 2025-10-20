from src.appointment_generator import AppointmentGenerator
import os
import pandas as pd

print("=" * 60)
print("HEALTHFLOW PARTITIONED DATA GENERATION")
print("=" * 60)

# Create generator
gen = AppointmentGenerator(seed=42)

# Generate full dataset
print("\n📊 Generating 5,000 appointments...")
df = gen.generate_appointments(
    n_records=5000,
    start_date='2024-01-01',
    end_date='2024-12-31'
)

# Add partition columns for easier grouping
df['partition_year'] = df['appointment_datetime'].dt.year
df['partition_month'] = df['appointment_datetime'].dt.month
df['partition_day'] = df['appointment_datetime'].dt.day

print("\n📁 Partitioning data by year/month/day...")
print("   (This organizes data like a professional data lake)")

# Count partitions
total_partitions = df.groupby(['partition_year', 'partition_month', 'partition_day']).ngroups
print(f"\n   Creating {total_partitions} date partitions...")

partition_count = 0

# Group by date and save separately
for (year, month, day), group in df.groupby(['partition_year', 'partition_month', 'partition_day']):
    partition_count += 1
    
    # Create directory structure: data/generated/year=2024/month=01/day=15/
    partition_path = f'data/generated/year={year}/month={month:02d}/day={day:02d}'
    os.makedirs(partition_path, exist_ok=True)
    
    # Save this day's data
    output_file = f'{partition_path}/appointments.csv'
    group.to_csv(output_file, index=False)
    
    # Show progress every 50 partitions
    if partition_count % 50 == 0:
        print(f"   ✓ Created {partition_count}/{total_partitions} partitions...")

print(f"\n✅ Partitioned {len(df):,} appointments into {total_partitions} daily files!")

print("\n📂 Example directory structure:")
print("data/generated/")
print("  └── year=2024/")
print("      ├── month=01/")
print("      │   ├── day=01/appointments.csv")
print("      │   ├── day=02/appointments.csv")
print("      │   ├── day=03/appointments.csv")
print("      │   └── ...")
print("      ├── month=02/")
print("      │   └── ...")
print("      └── month=12/")

print("\n📊 Summary Statistics:")
print(f"  Total appointments: {len(df):,}")
print(f"  Date range: {df['appointment_datetime'].min().date()} to {df['appointment_datetime'].max().date()}")
print(f"  Unique patients: {df['patient_id'].nunique():,}")
print(f"  Facilities: {df['facility_id'].nunique()}")
print(f"  Days with data: {total_partitions}")
print(f"  Avg appointments/day: {len(df)/total_partitions:.1f}")

print("\n📊 Wait times by department:")
dept_stats = df.groupby('department')['wait_time_minutes'].agg(['count', 'mean', 'min', 'max']).round(1)
print(dept_stats)

print("\n💡 Why partition like this?")
print("  ✓ Query specific dates without scanning all data")
print("  ✓ Upload only new data (today's partition)")
print("  ✓ Standard data lake structure (Hive-style partitioning)")
print("  ✓ Works with Spark, Snowflake, AWS Athena, etc.")
print("  ✓ Easy to delete old data (just remove old month folders)")

print("\n🎯 Next steps:")
print("  1. Explore: ls -R data/generated/")
print("  2. Check a file: cat data/generated/year=2024/month=01/day=01/appointments.csv")
print("  3. Upload to S3 with partitioning!")

print("\n✅ Data generation complete!")