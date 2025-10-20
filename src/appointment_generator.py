from faker import Faker
import pandas as pd
import random
from datetime import datetime, timedelta

class AppointmentGenerator:
    """Generate realistic hospital appointment data"""
    
    def __init__(self, seed=42):
        """
        Initialize the generator
        
        Args:
            seed (int): Random seed for reproducibility
        """
        random.seed(seed)
        self.fake = Faker()
        Faker.seed(seed)
        
        # Define hospital departments
        self.departments = [
            'Emergency',
            'Cardiology', 
            'Imaging',
            'Laboratory',
            'Orthopedics',
            'Neurology'
        ]
        
        # Define appointment types
        self.appointment_types = [
            'Emergency',
            'Scheduled',
            'Walk-in',
            'Follow-up'
        ]
        
        print("✓ AppointmentGenerator initialized")
    
    def generate_wait_time(self, department, hour, day_of_week):
        """
        Generate realistic wait time based on factors
        
        Args:
            department (str): Hospital department
            hour (int): Hour of day (0-23)
            day_of_week (int): Day of week (0=Monday, 6=Sunday)
        
        Returns:
            int: Wait time in minutes
        """
        # 1. Create base wait times dictionary
        base_wait = {
            'Emergency': 45,
            'Cardiology': 25,
            'Imaging': 15,
            'Laboratory': 10,
            'Orthopedics': 30,
            'Neurology': 35
        }
        
        # 2. Get base wait time for this department
        wait = base_wait.get(department, 20)  # Default 20 if not found
        
        # 3. Adjust for peak hours
        if hour in [8, 9, 10, 13, 14, 15]:
            wait = wait * 1.5
        
        # 4. Adjust for weekends
        if day_of_week in [5, 6]:  # Saturday, Sunday
            wait = wait * 0.7
        
        # 5. Add random variation (±30%)
        # Use random.uniform() to add variation
        # Hint: random.uniform(0.7, 1.3) gives values between 70% and 130%
        wait = wait * random.uniform(0.7, 1.3)
        
        # 6. Return as integer (minimum 5 minutes)
        return max(5, int(wait))
    
    def generate_appointments(self, n_records, start_date, end_date):
        """
        Generate appointment dataset
        
        Args:
            n_records (int): Number of appointments to generate
            start_date (str): Start date 'YYYY-MM-DD'
            end_date (str): End date 'YYYY-MM-DD'
        
        Returns:
            pd.DataFrame: Generated appointments
        """
        print(f"Generating {n_records:,} appointments...")
        
        # Convert string dates to datetime objects
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        
        records = []
        
        # Generate each appointment
        for i in range(n_records):
            # Progress indicator every 1000 records
            if i > 0 and i % 1000 == 0:
                print(f"  Generated {i:,} records...")
            
            # 1. Generate random appointment datetime
            appointment_datetime = self.fake.date_time_between(
                start_date=start,
                end_date=end
            )
            
            # 2. Extract date components
            hour = appointment_datetime.hour
            day_of_week = appointment_datetime.weekday()
            
            # 3. Pick facility and department
            facility_id = f"HOSP_{random.randint(1, 10):03d}"  # HOSP_001 to HOSP_010
            department = random.choice(self.departments)
            appointment_type = random.choice(self.appointment_types)
            
            # 4. Generate patient info
            patient_id = f"PAT_{self.fake.unique.random_int(min=100000, max=999999)}"
            patient_name = self.fake.name()
            patient_age = random.randint(18, 85)
            
            # Categorize age
            if patient_age < 30:
                age_group = '18-30'
            elif patient_age < 50:
                age_group = '30-50'
            elif patient_age < 70:
                age_group = '50-70'
            else:
                age_group = '70+'
            
            # 5. Generate wait time (using our method!)
            wait_time_minutes = self.generate_wait_time(department, hour, day_of_week)
            
            # 6. Calculate service start time
            service_start = appointment_datetime + timedelta(minutes=wait_time_minutes)
            
            # 7. Create appointment record
            record = {
                'appointment_id': f"APT_{i+1:08d}",
                'patient_id': patient_id,
                'patient_name': patient_name,
                'patient_age': patient_age,
                'patient_age_group': age_group,
                'facility_id': facility_id,
                'department': department,
                'appointment_type': appointment_type,
                'appointment_datetime': appointment_datetime,
                'service_start_datetime': service_start,
                'wait_time_minutes': wait_time_minutes,
                'hour_of_day': hour,
                'day_of_week': appointment_datetime.strftime('%A'),
                'month': appointment_datetime.month,
                'year': appointment_datetime.year
            }
            
            records.append(record)
        
        # Convert to DataFrame
        df = pd.DataFrame(records)
        
        print(f"\n✓ Generated {len(df):,} appointments")
        print(f"  Date range: {df['appointment_datetime'].min()} to {df['appointment_datetime'].max()}")
        print(f"  Facilities: {df['facility_id'].nunique()}")
        print(f"  Departments: {df['department'].nunique()}")
        print(f"  Avg wait time: {df['wait_time_minutes'].mean():.1f} minutes")
        
        return df