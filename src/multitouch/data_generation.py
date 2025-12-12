
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


def generate_synthetic_data(data_gen_path: str, unique_id_count: int = 500000):
    """
    This function will generate and write synthetic data for multi-touch 
    attribution modeling
    """
    
    # Open file and write out header row
    # Ensure directory exists is handled by caller or we assume it exists
    with open(data_gen_path, "w") as my_file:
        my_file.write("uid,time,interaction,channel,conversion\n")
        
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
        # channel_probability_weights = (20, 20, 20, 20, 20) # Unused in original code?
        
        # Generate list of random user IDs
        uid_list = []
        
        for _ in range(unique_id_count):
            uid_list.append(str(uuid.uuid4()).replace('-',''))
        
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
                d1 = datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S')
                
                if user_journey_end == 'conversion' and i == (steps_in_customer_journey-1):
                    my_file.write(uid+','+time_str+',conversion,'+final_channel+',1\n')
                else:
                    my_file.write(uid+','+time_str+',impression,'+next_step_in_user_journey+',0\n')
