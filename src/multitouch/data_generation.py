
import uuid
import random
from random import randrange
from datetime import datetime, timedelta

def _get_random_datetime(start, end):
    """
    This function will return a random datetime between two datetime 
    objects.
    """
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = randrange(int_delta)
    return start + timedelta(seconds=random_second)



import pandas as pd

def generate_synthetic_data(data_gen_path: str, unique_id_count: int = 500000):
    """
    This function will generate and write synthetic data for multi-touch 
    attribution modeling
    """
    
    # Specify the desired conversion rate for the full data set
    total_conversion_rate_for_campaign = .14
    
    # Specify the desired channels and channel-level conversion rates
    base_conversion_rate_per_channel = {'Social Network':.3, 
                                        'Search Engine Marketing':.2, 
                                        'Google Display Network':.1, 
                                        'Affiliates':.39, 
                                        'Email':0.01}
    
    
    channel_list = list(base_conversion_rate_per_channel.keys())
    
    base_conversion_weight = tuple(base_conversion_rate_per_channel.values())
    intermediate_channel_weight = (20, 30, 15, 30, 5)
    
    # Generate list of random user IDs
    # Note: Generating all UIDs first
    uid_list = [str(uuid.uuid4()).replace('-','') for _ in range(unique_id_count)]
    
    data_rows = []
    
    # Generate data / user journey for each unique user ID
    for uid in uid_list:
        user_journey_end = random.choices(['impression', 'conversion'], 
                                            (1-total_conversion_rate_for_campaign, total_conversion_rate_for_campaign), k=1)[0]
        
        steps_in_customer_journey = random.choice(range(1,10))
        
        d1 = datetime.strptime('5/17/2020 1:30 PM', '%m/%d/%Y %I:%M %p')
        d2 = datetime.strptime('6/10/2020 4:50 AM', '%m/%d/%Y %I:%M %p')

        final_channel = random.choices(channel_list, base_conversion_weight, k=1)[0]
        
        for i in range(steps_in_customer_journey):
            next_step_in_user_journey = random.choices(channel_list, weights=intermediate_channel_weight, k=1)[0] 
            time_str = str(_get_random_datetime(d1, d2))
            # Note: Sticking to string format for time to match original CSV behavior which was ingested as string then parsed.
            # However, for Parquet, we could use datetime objects directly.
            # To minimize downstream friction in schema inference, sticking to string for now, or ensure ingestion parses it.
            # Original ingestion parses "yyyy-MM-dd HH:mm:ss". str(datetime) creates that format.
            d1 = datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S')
            
            if user_journey_end == 'conversion' and i == (steps_in_customer_journey-1):
                data_rows.append({
                    "uid": uid,
                    "time": time_str,
                    "interaction": "conversion",
                    "channel": final_channel,
                    "conversion": 1
                })
            else:
                data_rows.append({
                    "uid": uid,
                    "time": time_str,
                    "interaction": "impression",
                    "channel": next_step_in_user_journey,
                    "conversion": 0
                })

    # Create DataFrame and write to Parquet
    print(f"Writing {len(data_rows)} rows to {data_gen_path}...")
    df = pd.DataFrame(data_rows)
    # Ensure columns order matches original CSV if that matters (it usually doesn't for Parquet which is columnar)
    df = df[["uid", "time", "interaction", "channel", "conversion"]]
    
    df.to_parquet(data_gen_path, index=False)

